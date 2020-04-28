/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.config.Configs;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.HostFailWorker;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskMoveAssignment;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.mq.CommandTaskStop;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.spawn.AvailabilityDomain;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.WithScore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.JobTaskState.IDLE;
import static com.addthis.hydra.job.JobTaskState.QUEUED;
import static com.addthis.hydra.job.JobTaskState.QUEUED_HOST_UNAVAIL;
import static com.addthis.hydra.job.JobTaskState.QUEUED_NO_SLOT;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_BALANCE_PARAM_PATH;

/**
 * A class in charge of balancing load among spawn's hosts.
 * General assumptions:
 * The boxes are not so asymmetrical that running a job on three boxes is slower than running it on a single box.
 * Jobs run faster when they have as few tasks grouped together on individual boxes as possible.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SpawnBalancer implements Codable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SpawnBalancer.class);

    private static final Set<JobTaskState> movableTaskStates = ImmutableSet.of(
            IDLE, QUEUED, QUEUED_HOST_UNAVAIL, QUEUED_NO_SLOT);

    // How often to update aggregate host statistics
    static final long AGGREGATE_STAT_UPDATE_INTERVAL = Parameter.intValue("spawnbalance.stat.update", 15 * 1000);

    // metrics
    private volatile double avgDiskPercentUsedDiff = 0;
    private volatile double minDiskPercentUsedDiff = 0;
    private volatile double maxDiskPercentUsedDiff = 0;
    private volatile double avgTaskPercentDiff = 0;
    private volatile double minTaskPercentDiff = 0;
    private volatile double maxTaskPercentDiff = 0;

    private final ConcurrentHashMap<String, HostScore> cachedHostScores;
    private final ReentrantLock aggregateStatisticsLock;
    private final AtomicBoolean autobalanceStarted;
    private final Cache<String, Boolean> recentlyAutobalancedJobs;
    private final Cache<String, Boolean> recentlyBalancedHosts;
    private final Cache<String, Boolean> recentlyReplicatedToHosts;
    private final SpawnBalancerTaskSizer taskSizer;
    private final Comparator<HostState> hostStateScoreComparator;
    private final Comparator<Job> jobAverageTaskSizeComparator;
    private final Comparator<HostState> hostStateReplicationSuitabilityComparator;
    private final ScheduledExecutorService taskExecutor;
    private static final Comparator<WithScore<JobTask>> taskComparator =
            Collections.reverseOrder(Comparator.comparingDouble(t -> t.score));

    final Spawn spawn;
    final HostManager hostManager;

    private volatile Set<String> activeJobIds;

    @FieldConfig
    private volatile SpawnBalancerConfig config;

    public SpawnBalancer(Spawn spawn, HostManager hostManager) {
        this.spawn = spawn;
        this.hostManager = hostManager;
        config = loadConfigFromDataStore(new SpawnBalancerConfig());
        taskExecutor = new ScheduledThreadPoolExecutor(
                2, new ThreadFactoryBuilder().setNameFormat("spawnBalancer-%d").build());
        taskExecutor.scheduleAtFixedRate(new AggregateStatUpdaterTask(this), AGGREGATE_STAT_UPDATE_INTERVAL,
                                         AGGREGATE_STAT_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        taskSizer = new SpawnBalancerTaskSizer(spawn, hostManager);
        cachedHostScores = new ConcurrentHashMap<>();
        aggregateStatisticsLock = new ReentrantLock();
        autobalanceStarted = new AtomicBoolean(false);
        recentlyAutobalancedJobs = CacheBuilder.newBuilder().expireAfterWrite(
                Parameter.intValue("spawnbalance.job.autobalance.interval.mins", 60 * 12), TimeUnit.MINUTES
        ).build();
        recentlyBalancedHosts = CacheBuilder.newBuilder().expireAfterWrite(
                Parameter.intValue("spawnbalance.host.balance.interval.mins", 3), TimeUnit.MINUTES
        ).build();
        recentlyReplicatedToHosts = CacheBuilder.newBuilder().expireAfterWrite(
                Parameter.intValue("spawnbalance.host.replicate.interval.mins", 15), TimeUnit.MINUTES
        ).build();
        hostStateScoreComparator = Comparator.comparingDouble(hostState -> this.getHostScoreCached(hostState.getHostUuid()));
        jobAverageTaskSizeComparator = (job, job1) -> {
            if ((job == null) || (job1 == null)) {
                return 0;
            } else {
                return Double.compare(job.calcAverageTaskSizeBytes(), job1.calcAverageTaskSizeBytes());
            }
        };
        hostStateReplicationSuitabilityComparator = (hostState, hostState1) -> {
            // Treat recently-replicated-to hosts as having fewer than their reported available bytes
            long availBytes = hostState.getAvailDiskBytes();
            long availBytes1 = hostState1.getAvailDiskBytes();
            if (recentlyReplicatedToHosts.getIfPresent(hostState.getHostUuid()) != null) {
                availBytes /= 2;
            }
            if (recentlyReplicatedToHosts.getIfPresent(hostState1.getHostUuid()) != null) {
                availBytes1 /= 2;
            }
            return -Double.compare(availBytes, availBytes1);
        };
        activeJobIds = new HashSet<>();
        this.initMetrics();
    }

    private void initMetrics() {
        SpawnBalancer.makeGauge("minDiskPercentUsedDiff", () -> minDiskPercentUsedDiff);
        SpawnBalancer.makeGauge("maxDiskPercentUsedDiff", () -> maxDiskPercentUsedDiff);
        SpawnBalancer.makeGauge("avgDiskPercentUsedDiff", () -> avgDiskPercentUsedDiff);
        SpawnBalancer.makeGauge("minTaskPercentDiff", () -> minTaskPercentDiff);
        SpawnBalancer.makeGauge("maxTaskPercentDiff", () -> maxTaskPercentDiff);
        SpawnBalancer.makeGauge("avgTaskPercentDiff", () -> avgTaskPercentDiff);
    }

    private static <T> void makeGauge(String name, Supplier<T> value) {
        Gauge<T> gauge = new Gauge<T>() {
            @Override public T value() {
                return value.get();
            }
        };
        Metrics.newGauge(SpawnBalancer.class, name, gauge);
    }

    /** Loads SpawnBalancerConfig from data store; if no data or failed, returns the default. */
    @VisibleForTesting
    protected SpawnBalancerConfig loadConfigFromDataStore(SpawnBalancerConfig defaultValue) {
        String configString = spawn.getSpawnDataStore().get(SPAWN_BALANCE_PARAM_PATH);
        if (!Strings.isNullOrEmpty(configString)) {
            try {
                return Configs.decodeObject(SpawnBalancerConfig.class, configString);
            } catch (Exception e) {
                log.warn("Failed to decode SpawnBalancerConfig", e);
            }
        }
        return defaultValue;
    }

    /**
     * A cached version of getHostScore that will keep host score calculation outside the main spawn thread
     *
     * @param hostId The host to measure
     * @return A non-negative number representing load.
     */
    public double getHostScoreCached(String hostId) {
        if (hostId == null) {
            return config.getDefaultHostScore();
        }
        HostScore score = cachedHostScores.get(hostId);
        if (score != null) {
            return score.getOverallScore();
        } else {
            return config.getDefaultHostScore();
        }
    }

    /** Start a thread that will perform autobalancing in the background if appropriate to do so */
    public synchronized void startAutobalanceTask() {
        if (autobalanceStarted.compareAndSet(false, true)) {
            taskExecutor.scheduleWithFixedDelay(new AutobalanceTask(this), config.getAutobalanceCheckInterval(),
                                                config.getAutobalanceCheckInterval(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Is this job unbalanced enough to warrant a rebalance?
     *
     * @param job   The job to check
     * @param hosts The hosts in the cluster
     * @return True if it is appropriate to rebalance the job
     */
    private synchronized boolean shouldAutobalanceJob(Job job, List<HostState> hosts) {
        if ((job == null)
            || (recentlyAutobalancedJobs.getIfPresent(job.getId()) != null)
            || (JobState.IDLE != job.getState())
            || job.getDontAutoBalanceMe()
            || (job.getRunCount() < 1)) {
            return false;
        }
        if (config.getAutoBalanceLevel() >= 2) {
            // At level 2, rebalance every job that hasn't already been rebalanced recently
            return true;
        }
        JobTaskItemByHostMap tasksByHost = generateTaskCountByHost(hosts, job.getCopyOfTasks());
        int maxPerHost = maxTasksPerHost(job, hosts.size());
        // If any host has sufficiently more or less than the expected fair share, this job is unbalanced.
        return (tasksByHost.findLeastTasksOnHost() <= (maxPerHost - 2)) ||
               (tasksByHost.findMostTasksOnHost() >= (maxPerHost + 1));
    }

    @Override public void close() {
        MoreExecutors.shutdownAndAwaitTermination(taskExecutor, 120, TimeUnit.SECONDS);
    }

    public void saveConfigToDataStore() {
        try {
            spawn.getSpawnDataStore().put(SPAWN_BALANCE_PARAM_PATH, CodecJSON.encodeString(config));
        } catch (Exception e) {
            log.warn("Failed to save SpawnBalancerConfig to data store", e);
        }
    }

    public void startTaskSizePolling() {
        taskSizer.startPolling(taskExecutor);
    }

    /**
     * Takes any arbitrary list of tasks and finds the best hosts to house all of them.
     *
     * @param tasks The tasks to assign to hosts
     * @param hosts All available hosts in the cluster
     * @return A map describing where to send each task
     */
    public Map<JobTask, String> assignTasksFromMultipleJobsToHosts(Collection<JobTask> tasks,
                                                                   Collection<HostState> hosts) {
        // Populate a map grouping the tasks by job ID.
        Map<String, List<JobTask>> tasksByJobID = new HashMap<>();
        for (JobTask task : tasks) {
            if (task.getJobUUID() != null) {
                Job job = spawn.getJob(task.getJobUUID());
                if (job != null) {
                    List<JobTask> taskList = tasksByJobID.computeIfAbsent(job.getId(), key -> new ArrayList<>());
                    taskList.add(task);
                }
            }
        }
        // This map of hosts to scores will be updated by every call to assignTasksFromSingleJobToHosts.
        Map<HostState, Double> hostScoreMap = generateHostStateScoreMap(hosts);
        // This map stores where to send each task.
        Map<JobTask, String> hostAssignments = new HashMap<>(tasks.size());
        for (Map.Entry<String, List<JobTask>> entry : tasksByJobID.entrySet()) {
            Map<JobTask, String> singleHostAssignments =
                    assignTasksFromSingleJobToHosts(entry.getValue(), hostScoreMap);
            hostAssignments.putAll(singleHostAssignments);
        }
        return hostAssignments;
    }

    /**
     * Function that creates a map of HostStates to their hostScores.
     *
     * @param hosts The hosts from spawn
     * @return A map taking each HostState to its score
     */
    public Map<HostState, Double> generateHostStateScoreMap(Collection<HostState> hosts) {
        final Map<HostState, Double> hostScores = new HashMap<>(hosts.size());
        for (HostState host : hosts) {
            if ((host != null) && host.isUp() && !host.isDead()) {
                double score = getHostScoreCached(host.getHostUuid());
                hostScores.put(host, score);
            }
        }
        return hostScores;
    }

    /**
     * Take a list of tasks belonging to a single job and find the best hosts to house these tasks.
     * A map of host scores is passed as a parameter so that it can be persisted across multiple calls to this function
     * by, for example, assignTasksFromMultipleJobsToHosts.
     */
    private Map<JobTask, String> assignTasksFromSingleJobToHosts(List<JobTask> tasks,
                                                                 Map<HostState, Double> storedHostScores) {
        if ((tasks == null) || tasks.isEmpty()) {
            return new HashMap<>();
        }

        // At this stage that the list is not empty
        // All tasks are from the same job
        HostCandidateIterator iterator = new HostCandidateIterator(spawn, tasks, storedHostScores);
        List<String> hostsToAssign = new ArrayList<>(tasks.size());
        for (JobTask task : tasks) {
            if(task == null) {
                continue;
            }
            List<String> targetHostList = iterator.getNewReplicaHosts(1, task, null, false);
            if(targetHostList.isEmpty()) {
                log.warn("[spawn.balancer] found no hosts eligible to receive new tasks");
                throw new RuntimeException("no eligible hosts for new task");
            }
            String targetHost = targetHostList.get(0);
            hostsToAssign.add(targetHost);
            storedHostScores.put(hostManager.getHostState(targetHost),
                                 storedHostScores.getOrDefault(targetHost, 0d) + config.getActiveTaskWeight());
        }
        return pairTasksAndHosts(tasks, hostsToAssign);
    }

    public boolean canReceiveNewTasks(HostState host) {
        if (host == null) {
            return false;
        }
        if (spawn.getHostFailWorker().getFailureState(host.getHostUuid()) != HostFailWorker.FailState.ALIVE) {
            return false;
        }
        return host.canMirrorTasks() && (host.getAvailDiskBytes() > config.getMinFreeDiskSpaceToRecieveNewTasks());
    }

    /**
     * Given a list of tasks and a list of potential hosts of the same size, pair them up.
     * Keep tasks on their existing hosts as much as possible.
     */
    private static Map<JobTask, String> pairTasksAndHosts(List<JobTask> tasks, Collection<String> hosts) {
        if ((tasks == null) || (hosts == null) || (tasks.size() != hosts.size())) {
            log.warn("[spawn.balancer] invalid call to pairTasksAndHosts: tasks={} hosts={}", tasks, hosts);
            return new HashMap<>(0);
        }
        Map<JobTask, String> rv = new HashMap<>(tasks.size());
        // For any task already living on a host in our hosts list, keep that task where it is.
        Collection<JobTask> unassignedTasks = new ArrayList<>();
        String jobID = tasks.get(0).getJobUUID();
        for (JobTask task : tasks) {
            if (!task.getJobUUID().equals(jobID)) {
                throw new RuntimeException(
                        "Illegal call to assignTasksFromSingleJobToHosts: not all tasks came from the same job");
            }
            String hostID = task.getHostUUID();
            if ((hostID != null) && hosts.contains(hostID)) {
                hosts.remove(hostID);
            } else {
                unassignedTasks.add(task);
            }
        }
        // Assign the remaining tasks by iterating down the remaining hosts.
        Iterator<String> hostIterator = hosts.iterator();
        for (JobTask task : unassignedTasks) {
            rv.put(task, hostIterator.next());
        }
        return rv;
    }

    public List<JobTaskMoveAssignment> pushTasksOffHostForFilesystemOkayFailure(HostState host, int moveLimit) {
        List<HostState> hosts = hostManager.listHostStatus(null);
        return pushTasksOffHost(host, hosts, false, moveLimit, false);
    }

    private List<JobTaskMoveAssignment> getMoveAssignments(HostState host, Collection<HostState> otherHosts,
                                                           boolean limitBytes, long byteLimit,
                                                           int moveLimit,
                                                           boolean obeyDontAutobalanceMe) {
        List<JobTaskMoveAssignment> moveAssignments = new ArrayList<>();

        for (JobTask task : findTasksToMove(host, obeyDontAutobalanceMe)) {
            long taskTrueSize = getTaskTrueSize(task);
            if (limitBytes && (taskTrueSize > byteLimit)) {
                continue;
            }
            JobTaskMoveAssignment assignment = moveTask(task, host.getHostUuid(), otherHosts);
            // we don't want to take up one of the limited rebalance slots
            // with an assignment that we know has no chance of happening
            // because either the assignment is null or the target host
            // for the assignment is null
            // add extra safeguard around selection of replica movement to ensure spread across availability domains
            if (assignment != null && assignment.getTargetUUID() != null &&
                isTaskSpreadOutAcrossAd(host.getHostLocation(),
                                        hostManager.getHostLocationForHost(assignment.getTargetUUID()), task)) {
                moveAssignments.add(assignment);
                byteLimit -= taskTrueSize;
            }
            if(moveAssignments.size() >= moveLimit) {
                break;
            }
        }
        return moveAssignments;
    }

    /* Push/pull the tasks on a host to balance its disk, obeying an overall limit on the number of tasks/bytes to
    move */
    private List<JobTaskMoveAssignment> pushTasksOffHost(HostState host,
                                                         Collection<HostState> otherHosts,
                                                         boolean limitBytes,
                                                         int moveLimit,
                                                         boolean obeyDontAutobalanceMe) {
        List<JobTaskMoveAssignment> moveAssignments = getMoveAssignments(host,
                                                                         otherHosts,
                                                                         limitBytes,
                                                                         config.getBytesMovedFullRebalance(),
                                                                         moveLimit,
                                                                         obeyDontAutobalanceMe
        );
        markRecentlyReplicatedTo(moveAssignments);
        moveAssignments.addAll(purgeMisplacedTasks(host, moveLimit));
        return moveAssignments;
    }

    /* Look through a hoststate to find tasks that don't correspond to an actual job or are on the wrong host */
    private List<JobTaskMoveAssignment> purgeMisplacedTasks(HostState host, int deleteLimit) {
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        for (JobKey key : host.allJobKeys()) {
            if (spawn.getJob(key) == null) {
                // Nonexistent job
                rv.add(new JobTaskMoveAssignment(key, host.getHostUuid(), null, false, true));
            } else {
                // Task has a copy on the wrong host. Do a fixTaskDir to ensure we aren't deleting the only remaining
                // copy
                JobTask task = spawn.getTask(key);
                if (!host.getHostUuid().equals(task.getHostUUID()) && !task.hasReplicaOnHost(host.getHostUuid())) {
                    spawn.fixTaskDir(key.getJobUuid(), key.getNodeNumber(), false, false);
                    deleteLimit -= 1;
                }
            }
            if (rv.size() >= deleteLimit) {
                break;
            }
        }
        return rv;
    }

    /**
     * Sorts the host based on their available disk space, from most space available to least
     *
     * @param hosts - the hosts to sort
     * @return the sorted list of hosts
     */
    @VisibleForTesting
    protected List<HostState> sortHostsByDiskSpace(Collection<HostState> hosts) {
        List<HostState> hostList = new ArrayList<>(hosts);
        removeDownHosts(hostList);
        Collections.sort(hostList, hostStateReplicationSuitabilityComparator);
        return hostList;
    }

    /**
     * Find suitable tasks to move off a host.
     *
     * @param host                  The hoststate to move tasks off of
     * @param obeyDontAutobalanceMe If true, obey the job parameter dontAutobalanceMe. See notes below.
     * @return A list of tasks that are suitable to be moved
     */
    private Iterable<JobTask> findTasksToMove(HostState host, boolean obeyDontAutobalanceMe) {
        Collection<JobTask> rv = new ArrayList<>();
        if (host != null) {
            String hostId = host.getHostUuid();
            for (JobKey jobKey : host.allJobKeys()) {
                JobTask task = spawn.getTask(jobKey);
                Job job = spawn.getJob(jobKey);
                // Only add non-null tasks that are either idle or queued
                // Only add tasks that are supposed to live on the specified host.
                if ((job != null) && (task != null) && isInMovableState(task) &&
                    (hostId.equals(task.getHostUUID()) || task.hasReplicaOnHost(hostId))) {
                    if (obeyDontAutobalanceMe && job.getDontAutoBalanceMe()) {
                        // obeyDontAutobalanceMe is set to false when spawn is doing a filesystem-okay host failure.
                        // In this case, spawn needs to move the task even if the job owner specified no swapping,
                        // because the box is likely to be ailing/scheduled for decommission.
                        // All rebalancing actions use obeyDontAutobalanceMe=true and will conform to the job owner's
                        // wishes.
                        continue;
                    }
                    rv.add(task);
                }
            }
        }
        return rv;
    }

    public long getTaskTrueSize(JobTask task) {
        return taskSizer.estimateTrueSize(task);
    }

    /**
     * Given an ordered list of hosts, move the task to a suitable host
     *
     * @param task       The task to move
     * @param fromHostId The task to move the host from
     * @param otherHosts The potential target hosts
     * @return An assignment describing how to move the task
     */
    @Nullable private JobTaskMoveAssignment moveTask(JobTask task,
                                                     String fromHostId,
                                                     Collection<HostState> otherHosts) {
        String taskHost = task.getHostUUID();
        boolean live = taskHost.equals(fromHostId);
        if (!live && !task.hasReplicaOnHost(fromHostId)) {
            return null;
        }
        Map<HostState, Double> scoreMap = generateHostStateScoreMap(otherHosts);
        Job job = spawn.getJob(task.getJobUUID());
        HostCandidateIterator iterator = new HostCandidateIterator(spawn, job.getCopyOfTasks(), scoreMap);
        List<String> newHostList = iterator.getNewReplicaHosts(1, task, taskHost, true);

        if(!newHostList.isEmpty()) {
            return new JobTaskMoveAssignment(task.getJobKey(), fromHostId, newHostList.get(0), !live, false);
        }
        return null;
    }

    @VisibleForTesting
    protected void markRecentlyReplicatedTo(List<JobTaskMoveAssignment> moveAssignments) {
        if (moveAssignments != null) {
            moveAssignments.stream()
                           .filter(assignment -> assignment.getTargetUUID() != null)
                           .map(assignment -> assignment.getTargetUUID())
                           .forEach(targetHostUUID -> recentlyReplicatedToHosts.put(targetHostUUID, true));
        }
    }

    private static void removeDownHosts(Iterable<HostState> hosts) {
        Iterator<HostState> hostIter = hosts.iterator();
        while (hostIter.hasNext()) {
            HostState host = hostIter.next();
            if (host.isDead() || !host.isUp()) {
                hostIter.remove();
            }
        }
    }

    public static boolean isInMovableState(JobTask task) {
        return (task != null) && movableTaskStates.contains(task.getState());
    }

    /**
     * Is it acceptable to put a replica of this task on this host? (task can't have a live and replica version on
     * host)
     *
     * @param hostCandidate The host that is being considered to house the new replica
     * @param task          The task that might be replicated to that host
     * @return True if it is okay to put a replica on the host
     */
    boolean okToPutReplicaOnHost(HostState hostCandidate, JobTask task) {
        Job job;
        String hostId;
        if ((hostCandidate == null) || ((hostId = hostCandidate.getHostUuid()) == null) ||
            !canReceiveNewTasks(hostCandidate) || ((job = spawn.getJob(task.getJobKey())) == null) ||
            !hostCandidate.getMinionTypes().contains(job.getMinionType())) {
            return false;
        }
        if (spawn.getHostFailWorker().getFailureState(hostId) != HostFailWorker.FailState.ALIVE) {
            return false;
        }
        HostState taskHost = hostManager.getHostState(task.getHostUUID());
        /* Protect against npe in case the existing host has disappeared somehow */
        String existingHost = (taskHost != null) ? taskHost.getHost() : null;
        /* in non-local-stack, prevent replicates to same host (multi-minion-per-host-setup) */
        if (!config.allowSameHostReplica() && hostCandidate.getHost().equals(existingHost)) {
            return false;
        }
        /* don't let the same minion have duplicate tasks */
        if (task.getHostUUID().equals(hostCandidate.getHostUuid()) ||
            task.hasReplicaOnHost(hostCandidate.getHostUuid())) {
            return false;
        }
        /* try not to put a task on a host if it would almost fill the host */
        if (taskSizer.estimateTrueSize(task) > (config.getHostDiskFactor() * hostCandidate.getAvailDiskBytes())) {
            return false;
        }
        return true;
    }

    /**
     * Decide if spawn is in a good state to perform an autobalance.
     *
     * @return True if it is okay to autobalance
     */
    public boolean okayToAutobalance() {
        // Don't autobalance if it is disabled, spawn is quiesced, or the failure queue is non-empty
        log.info(
                "Auto balance level: {}, Quiesce status: {}, Queued hosts to fail: {}",
                config.getAutoBalanceLevel(),
                spawn.getSystemManager().isQuiesced(),
                spawn.getHostFailWorker().queuedHosts().size()
        );
        if ((config.getAutoBalanceLevel() == 0) ||
            spawn.getSystemManager().isQuiesced() ||
            spawn.getHostFailWorker().queuedHosts().size() > 0) {
            log.info("Not Okay to autobalance");
            return false;
        }
        // Don't autobalance if there are still jobs in rebalance state
        for (Job job : spawn.listJobs()) {
            if (JobState.REBALANCE.equals(job.getState())) {
                log.warn("Auto rebalance blocked by job (enabled = {}) in rebalance: {}", job.isEnabled(), job.getId());
                return false;
            }
        }
        log.info("Okay to autobalance");
        return true;
    }

    /**
     * Find some task move assignments to autobalance the cluster
     *
     * @param type   Whether to balance hosts or jobs
     * @param weight Whether to balance light, medium, or heavy items of the chosen type
     * @return A list of assignments to perform the specified balancing operation
     */
    @Nullable public List<JobTaskMoveAssignment> getAssignmentsForAutoBalance(RebalanceType type,
                                                                              RebalanceWeight weight) {
        List<HostState> hosts = hostManager.getLiveHosts(null);
        switch (type) {
            case HOST:
                if (hosts.isEmpty()) {
                    return null;
                }
                List<HostState> hostsSorted = new ArrayList<>(hosts);
                Collections.sort(hostsSorted, hostStateScoreComparator);
                HostState hostToBalance = hostsSorted.get(getWeightedElementIndex(hostsSorted.size(), weight));
                return getAssignmentsToBalanceHost(hostToBalance,
                                                   hostManager.listHostStatus(hostToBalance.getMinionTypes()));
            case JOB:
                List<Job> autobalanceJobs = getJobsToAutobalance(hosts);
                if ((autobalanceJobs == null) || autobalanceJobs.isEmpty()) {
                    return null;
                }
                Job jobToBalance = autobalanceJobs.get(getWeightedElementIndex(autobalanceJobs.size(), weight));
                recentlyAutobalancedJobs.put(jobToBalance.getId(), true);
                return getAssignmentsForJobReallocation(jobToBalance, -1,
                                                        hostManager.listHostStatus(jobToBalance.getMinionType()));
            default:
                throw new IllegalArgumentException("unknown rebalance type " + type);
        }
    }

    public List<Job> getJobsToAutobalance(List<HostState> hosts) {
        List<Job> autobalanceJobs = new ArrayList<>();
        for (Job job : spawn.listJobs()) {
            if (shouldAutobalanceJob(job, hosts)) {
                autobalanceJobs.add(job);
            }
        }
        Collections.sort(autobalanceJobs, jobAverageTaskSizeComparator);
        return autobalanceJobs;
    }

    public Map<Integer, List<String>> getAssignmentsForNewReplicas(IJob job) {
        return getAssignmentsForNewReplicas(job, -1);
    }

    /**
     * Given a job, decide where to put additional replicas so that every task will have its full quantity of replicas.
     *
     * @param job    The job in question (not altered)
     * @param taskID The task that needs replicas, or -1 for all tasks
     * @return Map sending nodeid => list of host IDs for which to make new replicas
     */
    public Map<Integer, List<String>> getAssignmentsForNewReplicas(IJob job, int taskID) {
        Map<Integer, List<String>> rv = new HashMap<>();
        if (job == null) {
            return rv;
        }
        int replicaCount = job.getReplicas();
        List<JobTask> tasks = (taskID > 0) ? Collections.singletonList(job.getTask(taskID)) : job.getCopyOfTasks();
        // Note: we will get an up-to-date list of up and not dead hosts here
        Map<HostState, Double> scoreMap =
                generateHostStateScoreMap(hostManager.listHostStatus(job.getMinionType()));
        HostCandidateIterator hostCandidateIterator = new HostCandidateIterator(spawn,
                                                                                job.getCopyOfTasks(),
                                                                                scoreMap);
        for (JobTask task : tasks) {
            int numExistingReplicas = task.getReplicas() != null ? task.getReplicas().size() : 0;
            List<String> hostIDsToAdd =
                    hostCandidateIterator.getNewReplicaHosts(replicaCount - numExistingReplicas, task);
            if (!hostIDsToAdd.isEmpty()) {
                rv.put(task.getTaskID(), hostIDsToAdd);
            }
        }
        return rv;
    }

    public void requestJobSizeUpdate(String jobId, int taskId) {
        taskSizer.requestJobSizeFetch(jobId, taskId);
    }

    public SpawnBalancerConfig getConfig() {
        return config;
    }

    public void setConfig(SpawnBalancerConfig config) {
        this.config = config;
    }

    public void clearRecentlyRebalancedHosts() {
        recentlyBalancedHosts.invalidateAll();
    }

    /**
     * Given a list of tasks with lives/replicas on a failed host, fix all of the tasks
     *
     * @param hosts      All available hosts in the cluster of the appropriate type
     * @param failedHost The id of the host being failed
     */
    public void fixTasksForFailedHost(List<HostState> hosts, String failedHost) {
        List<HostState> copyOfHosts = new ArrayList<>(hosts);
        List<JobTask> tasks = findAllTasksAssignedToHost(failedHost);
        List<JobTask> sortedTasks = new ArrayList<>(tasks);
        sortedTasks.sort(Comparator.comparingLong(taskSizer::estimateTrueSize));
        copyOfHosts.sort(hostStateScoreComparator);
        Collection<String> modifiedJobIds = new HashSet<>();
        for (JobTask task : sortedTasks) {
            modifiedJobIds.add(task.getJobUUID());
            try {
                attemptFixTaskForFailedHost(task, copyOfHosts, failedHost);
            } catch (Exception ex) {
                log.warn("Warning: failed to recover task {}", task.getJobKey(), ex);
            }
        }
        for (String jobId : modifiedJobIds) {
            try {
                spawn.updateJob(spawn.getJob(jobId));
            } catch (Exception e) {
                log.warn("Warning: failed to update job: {}", jobId, e);
            }
        }
    }

    private List<JobTask> findAllTasksAssignedToHost(String failedHostUUID) {
        List<JobTask> rv = new ArrayList<>();
        // Why?
        spawn.acquireJobLock();
        try {
            for (Job job : spawn.listJobs()) {
                if (job != null) {
                    for (JobTask task : job.getCopyOfTasks()) {
                        if ((task != null) &&
                            (task.getHostUUID().equals(failedHostUUID) || task.hasReplicaOnHost(failedHostUUID))) {
                            rv.add(task);
                        }
                    }
                }
            }
            return rv;
        } finally {
            spawn.releaseJobLock();
        }
    }

    /**
     * For a particular task with a copy on a failed host, attempt to find a suitable replica; mark it degraded
     * otherwise
     *
     * @param task           The task to modify
     * @param hosts          A list of available hosts
     * @param failedHostUuid The host being failed
     */
    private void attemptFixTaskForFailedHost(JobTask task, Collection<HostState> hosts, String failedHostUuid) {
        Iterator<HostState> hostIterator = hosts.iterator();
        Job job;
        if ((task == null) || (task.getJobUUID() == null) || ((job = spawn.getJob(task.getJobUUID())) == null)) {
            log.warn("Skipping nonexistent job for task {} during host fail.", task);
            return;
        }
        if (!task.getHostUUID().equals(failedHostUuid) && !task.hasReplicaOnHost(failedHostUuid)) {
            // This task was not actually assigned to the failed host. Nothing to do.
            return;
        }
        if (!spawn.isNewTask(task) && ((task.getReplicas() == null) || task.getReplicas().isEmpty())) {
            log.warn("Found no replica for task {}", task.getJobKey());
            job.setState(JobState.DEGRADED, true);
            return;
        }
        while (hostIterator.hasNext()) {
            HostState host = hostIterator.next();
            if (host.getHostUuid().equals(failedHostUuid)) {
                continue;
            }
            if (host.canMirrorTasks() && okToPutReplicaOnHost(host, task) &&
                isTaskSpreadOutAcrossAd(hostManager.getHostLocationForHost(task.getHostUUID()),
                                        hostManager.getHostLocationForHost(host.getHostUuid()),
                                        task)) {
                // Host found! Move this host to the end of the host list so we don't immediately pick it again
                hostIterator.remove();
                hosts.add(host);
                executeHostFailureRecovery(task, failedHostUuid, host);
                return;
            }
        }
        log.warn("Failed to find a host that could hold {} after host failure", task.getJobKey());
        job.setState(JobState.DEGRADED, true);
    }

    /**
     * Modify the live/replica copies of a task to handle a failed host
     *
     * @param task           The task to be modified
     * @param failedHostUuid The host being failed
     * @param newReplicaHost A host that will receive a new copy of the data
     */
    private void executeHostFailureRecovery(JobTask task, String failedHostUuid, CoreMessage newReplicaHost) {
        boolean liveOnFailedHost = task.getHostUUID().equals(failedHostUuid);
        String newReplicaUuid = newReplicaHost.getHostUuid();
        if (liveOnFailedHost) {
            if (spawn.isNewTask(task)) {
                // Task has never run before. Just switch to the new host.
                task.setHostUUID(newReplicaHost.getHostUuid());
            } else {
                // Send a kill message if the task is running on the failed host
                spawn.sendControlMessage(
                        new CommandTaskStop(failedHostUuid, task.getJobUUID(), task.getTaskID(), 0, true, false));
                // Find a replica, promote it, and tell it to replicate to the new replica on completion
                String chosenReplica = task.getReplicas().get(0).getHostUUID();
                task.replaceReplica(chosenReplica, newReplicaUuid);
                task.setHostUUID(chosenReplica);
                spawn.replicateTask(task, Collections.singletonList(newReplicaUuid));
            }
        } else {
            // Replace the replica on the failed host with one on a new host
            task.replaceReplica(failedHostUuid, newReplicaUuid);
            if (!spawn.isNewTask(task)) {
                spawn.replicateTask(task, Collections.singletonList(newReplicaUuid));
            }
        }
    }

    /**
     * Makes the specified number of tasks for the given job ID, and assigns each task to a suitable host.
     *
     * @param jobID     the job in question
     * @param taskCount how many tasks should be created
     * @param hosts     a current set of hosts
     * @return a list of tasks with HostID set.
     */
    public List<JobTask> generateAssignedTasksForNewJob(String jobID, int taskCount, Collection<HostState> hosts)
            throws Exception {
        List<JobTask> tasks = generateUnassignedTaskList(jobID, taskCount);
        Map<JobTask, String> hostAssignments =
                assignTasksFromSingleJobToHosts(tasks, generateHostStateScoreMap(hosts));
        List<JobTask> rv = new ArrayList<>(tasks.size());
        for (Map.Entry<JobTask, String> entry : hostAssignments.entrySet()) {
            JobTask task = entry.getKey();
            String hostID = entry.getValue();
            if (hostID == null) {
                throw new RuntimeException("Unable to allocate job tasks because no suitable host was found");
            }
            task.setHostUUID(hostID);
            rv.add(task);
        }
        return rv;
    }

    /**
     * Makes a list of empty tasks which will be assigned to hosts later.
     */
    private static List<JobTask> generateUnassignedTaskList(String jobID, int taskCount) {
        List<JobTask> rv = new ArrayList<>(Math.max(0, taskCount));
        for (int i = 0; i < taskCount; i++) {
            JobTask task = new JobTask();
            task.setJobUUID(jobID);
            task.setTaskID(i);
            rv.add(task);
        }
        return rv;
    }

    /**
     * Advises Spawn how to reallocate a job, sending some tasks to different hosts
     *
     * @param job         The job being reallocated
     * @param tasksToMove The number of tasks to move. If <= 0, use the default.
     * @param hosts       The available target hosts
     * @return a map assigning a good host for each jobtask
     */
    public List<JobTaskMoveAssignment> getAssignmentsForJobReallocation(Job job,
                                                                        int tasksToMove,
                                                                        List<HostState> hosts) {
        int maxTasksToMove = (tasksToMove > 0) ? tasksToMove : config.getTasksMovedFullRebalance();
        List<JobTaskMoveAssignment> candidateAssignments = new ArrayList<>();
        // Count the number of tasks per host
        JobTaskItemByHostMap tasksByHost = generateTaskCountByHost(hosts, job.getCopyOfTasks());
        // Find the max number of tasks each host should have
        int maxPerHost = maxTasksPerHost(job, tasksByHost.size());
        if (log.isDebugEnabled()) {
            log.debug("Rebalancing job: {} maxTasksToMove={} maxPerHost={}", job.getId(), maxTasksToMove, maxPerHost);
        }
        while (candidateAssignments.size() < maxTasksToMove) {
            MoveAssignmentList moves = null;
            List<String> hostsSorted = tasksByHost.generateHostsSorted();
            String hostWithMost = hostsSorted.get(hostsSorted.size() - 1);
            String hostWithLeast = hostsSorted.get(0);
            int mostTasksOnHost = tasksByHost.findMostTasksOnHost();
            int leastTasksOnHost = tasksByHost.findLeastTasksOnHost();
            boolean isExtremeHost = isExtremeHost(hostWithMost, true, true);
            if (log.isDebugEnabled()) {
                log.debug(
                        "hostsSorted.size={} hostWithMost:{} hostWithLeast:{} mostTasksOnHost: {} leastTasksOnHost: {}",
                        hostsSorted.size(), hostWithMost, hostWithLeast, mostTasksOnHost, leastTasksOnHost);
            }

            // If any host has more than the max number, move some tasks off that host
            if (mostTasksOnHost > maxPerHost) {
                moves = moveTasksOffHost(tasksByHost, maxPerHost, 1, -1, hostWithMost);
            } else if (leastTasksOnHost < (maxPerHost -
                                           1)) { // If a host has significantly fewer than the max number, move some
                // tasks onto that host
                moves = moveTasksOntoHost(tasksByHost, maxPerHost, 1, -1, hostWithLeast);
            } else if (isExtremeHost) { // If a host with many tasks is heavily weighted, move a task off that host
                moves = moveTasksOffHost(tasksByHost, maxPerHost, 1, -1, hostWithMost);
            }
            if ((moves == null) || moves.isEmpty()) {
                break;
            } else {
                candidateAssignments.addAll(moves.getList());
            }
        }
        if (candidateAssignments.size() > maxTasksToMove) {
            candidateAssignments = candidateAssignments.subList(0, maxTasksToMove);
        }
        candidateAssignments = removeDuplicateAssignments(candidateAssignments);
        return pruneTaskReassignments(candidateAssignments);
    }

    /**
     * Advises Spawn how to reallocate a host, pushing or pulling jobs to balance the number of tasks run by each
     * machine.
     *
     * @param host  The particular host to consider
     * @param hosts All available hosts; should include host
     * @return a (possibly empty) map specifying some tasks and advised destinations for those tasks
     */
    public List<JobTaskMoveAssignment> getAssignmentsToBalanceHost(HostState host, List<HostState> hosts) {
        String hostID = host.getHostUuid();
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        if ((hosts == null) || hosts.isEmpty()) {
            log.warn("[spawn.balancer] {} reallocation failed: host list empty", hostID);
            return rv;
        }
        List<HostState> sortedHosts = sortHostsByDiskSpace(hosts);
        int numAlleviateHosts = (int) Math.ceil(sortedHosts.size() * config.getAlleviateHostPercentage());
        HostFailWorker.FailState failState = spawn.getHostFailWorker().getFailureState(hostID);
        if ((failState == HostFailWorker.FailState.FAILING_FS_OKAY) || isExtremeHost(hostID, true, true) ||
            (host.getAvailDiskBytes() < config.getMinFreeDiskSpaceToRecieveNewTasks())) {
            // Host disk is overloaded
            log.info("[spawn.balancer] {} categorized as overloaded host; looking for tasks to push off of it", hostID);
            List<HostState> lightHosts = sortedHosts.subList(0, numAlleviateHosts);
            List<JobTaskMoveAssignment> moveAssignments = pushTasksOffHost(host, lightHosts, true, config.getTasksMovedFullRebalance(), true);
            rv.addAll(moveAssignments);
        } else if (isExtremeHost(hostID, true, false)) {
            // Host disk is underloaded
            log.info("[spawn.balancer] {} categorized as underloaded host; looking for tasks to pull onto it", hostID);
            List<HostState> heavyHosts =
                    Lists.reverse(sortedHosts.subList(sortedHosts.size() - numAlleviateHosts, sortedHosts.size()));
            List<JobTaskMoveAssignment> moveAssignments = pushTasksOntoHost(host, heavyHosts);
            rv.addAll(moveAssignments);
        } else if (isExtremeHost(hostID, false, true)) {
            // Host is overworked
            log.info("[spawn.balance] {} categorized as overworked host; looking for tasks to push off it", hostID);
            rv.addAll(balanceActiveJobsOnHost(host, hosts));
        }
        if (rv.isEmpty()) {
            rv.addAll(balanceActiveJobsOnHost(host, hosts));
        }
        return pruneTaskReassignments(rv);
    }

    /**
     * Sorts the hosts based on their load as measured by hostScores, lightest to heaviest.
     *
     * @param hosts - the hosts to sort
     * @return the sorted list of hosts, light to heavy.
     */
    public List<HostState> sortHostsByActiveTasks(Collection<HostState> hosts) {
        List<HostState> hostList = new ArrayList<>(hosts);
        removeDownHosts(hostList);
        Collections.sort(hostList, Comparator.comparingDouble(this::countTotalActiveTasksOnHost));
        return hostList;
    }

    private int countTotalActiveTasksOnHost(HostState host) {
        int count = 0;
        if (host != null) {
            host.generateJobTaskCountMap();
            Set<String> jobIds = getActiveJobIds();
            for (String jobId : jobIds) {
                count += host.getTaskCount(jobId);
            }
        }
        return count;
    }

    private Set<String> getActiveJobIds() {
        return activeJobIds;
    }

    /** Updates activeJobIds atomically */
    @VisibleForTesting
    void updateActiveJobIDs() {
        Collection<Job> jobs = spawn.listJobsConcurrentImmutable();
        if ((jobs != null) && !jobs.isEmpty()) {
            Set<String> jobIds = new HashSet<>(getActiveJobIds().size());
            for (Job job : jobs) {
                if (isWellFormedAndActiveJob(job)) {
                    jobIds.add(job.getId());
                }
            }
            this.activeJobIds = jobIds;
        }
    }

    /**
     * Is this job non-null and has it run recently?
     */
    private boolean isWellFormedAndActiveJob(IJob job) {
        long earliestActiveTime = JitterClock.globalTime() - config.getActiveTaskMilliCutoff();
        return (job != null) && (job.getStartTime() != null) && (job.getStartTime() > earliestActiveTime);
    }

    /**
     * Given a job, for each task, remove duplicate replicas, replicas pointing at the live host
     *
     * @param job The job to modify
     */
    public void removeInvalidReplicas(IJob job) {
        if ((job != null) && (job.getCopyOfTasks() != null)) {
            List<JobTask> tasks = job.getCopyOfTasks();
            for (JobTask task : tasks) {
                List<JobTaskReplica> newReplicas = new ArrayList<>(job.getReplicas());
                if (task.getReplicas() != null) {
                    Iterable<JobTaskReplica> oldReplicas = new ArrayList<>(task.getReplicas());
                    for (JobTaskReplica replica : oldReplicas) {
                        Collection<String> replicasSeen = new ArrayList<>();
                        String replicaHostID = replica.getHostUUID();
                        if (hostManager.getHostState(replicaHostID) == null) {
                            log.warn("[spawn.balancer] removing replica for missing host {}", replicaHostID);
                        } else if (replicaHostID.equals(task.getHostUUID()) || replicasSeen.contains(replicaHostID)) {
                            log.warn("[spawn.balancer] removing erroneous replica for {} on {}",
                                     task.getJobKey(), replicaHostID);
                        } else if (!config.allowSameHostReplica() && onSameHost(replicaHostID, task.getHostUUID())) {
                            log.warn("[spawn.balancer] removing replica on same host for {}live={} replica={}",
                                     task.getJobKey(), task.getHostUUID(), replicaHostID);
                        } else {
                            replicasSeen.add(replicaHostID);
                            newReplicas.add(replica);
                        }
                    }
                }
                task.setReplicas(newReplicas);
            }
        }
    }

    private boolean onSameHost(String hostID1, String hostID2) {
        HostState host1 = hostManager.getHostState(hostID1);
        HostState host2 = hostManager.getHostState(hostID2);
        if ((host1 == null) || (host2 == null)) {
            return false;
        } else {
            return host1.getHost().equals(host2.getHost());
        }
    }

    /**
     * Check the live and replica hosts for a given task to see whether any of these hosts has a nearly-full disk
     *
     * @param task The task to check
     * @return True if at least one host is near full
     */
    protected boolean hasFullDiskHost(JobTask task) {
        Collection<HostState> hostsToCheck = new ArrayList<>();
        hostsToCheck.add(hostManager.getHostState(task.getHostUUID()));
        if (task.getReplicas() != null) {
            for (JobTaskReplica replica : task.getReplicas()) {
                if (replica != null) {
                    hostsToCheck.add(hostManager.getHostState(replica.getHostUUID()));
                }
            }
            for (HostState host : hostsToCheck) {
                if ((host != null) && isDiskFull(host)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Does this host have a nearly full disk?
     *
     * @param host The host to check
     * @return True if the disk is nearly full
     */
    public boolean isDiskFull(@Nullable HostState host) {
        if (host == null) {
            return false;
        }
        long freeSpace = host.getAvailDiskBytes();
        boolean full = freeSpace <= config.getMinFreeDiskSpaceToRunJobs();
        if (full) {
            log.warn("[spawn.balancer] Host {} with uuid {} is nearly full, with {} GB free disk space", host.getHost(),
                     host.getHostUuid(), freeSpace / 1_000_000_000);
        }
        return full;
    }

    /**
     * Update SpawnBalancer's cluster-wide metrics, including host scores and active jobs.
     *
     * @param hosts A list of HostStates
     */
    protected void updateAggregateStatistics(List<HostState> hosts) {
        aggregateStatisticsLock.lock();
        try {
            updateActiveJobIDs();
            double maxMeanActive = -1;
            double maxDiskPercentUsed = -1;
            double minDiskPercentUsed = 1;
            double sumDiskPercentUsed = 0;
            double maxTaskPercent = 0;
            double minTaskPercent = 1;
            double sumTaskPercent = 0;
            for (HostState host : hosts) {
                maxMeanActive = Math.max(maxMeanActive, host.getMeanActiveTasks());
                double diskPercentUsed = host.getDiskUsedPercent();
                sumDiskPercentUsed += diskPercentUsed;
                maxDiskPercentUsed = Math.max(diskPercentUsed, maxDiskPercentUsed);
                minDiskPercentUsed = Math.min(diskPercentUsed, minDiskPercentUsed);
                double taskPercent = host.getMeanActiveTasks();
                sumTaskPercent += taskPercent;
                maxTaskPercent = Math.max(taskPercent, maxTaskPercent);
                minTaskPercent = Math.min(taskPercent, minTaskPercent);
            }
            int numScores = hosts.size();
            double avgDiskPercentUsed = sumDiskPercentUsed / (double) numScores;
            double sumDiskPercentUsedDiff = 0;
            double avgTaskPercent = sumTaskPercent / (double) numScores;
            double sumTaskPercentDiff = 0;
            for (HostState host : hosts) {
                HostScore score = calculateHostScore(host, maxMeanActive, maxDiskPercentUsed);
                cachedHostScores.put(host.getHostUuid(), score);

                // update average metrics
                double diskDiff = Math.abs(avgDiskPercentUsed - host.getDiskUsedPercent());
                sumDiskPercentUsedDiff += diskDiff;
                double taskDiff = score.getScoreValue(false);
                sumTaskPercentDiff += Math.abs(avgTaskPercent - taskDiff);
            }
            avgDiskPercentUsedDiff = sumDiskPercentUsedDiff / (double) numScores;
            avgTaskPercentDiff = sumTaskPercentDiff / (double) numScores;
            minDiskPercentUsedDiff = avgDiskPercentUsed - minDiskPercentUsed;
            maxDiskPercentUsedDiff = maxDiskPercentUsed - avgDiskPercentUsed;
            minTaskPercentDiff = avgTaskPercent - minTaskPercent;
            maxTaskPercentDiff = maxTaskPercent - avgTaskPercent;
        } finally {
            aggregateStatisticsLock.unlock();
        }
    }

    private HostScore calculateHostScore(HostState host, double clusterMaxMeanActive, double clusterMaxDiskUsed) {
        double meanActive = host.getMeanActiveTasks();
        // Get percentage of allowed disk space used (max - min free space = allowed)
        double diskUsedPercentModified = host.getDiskUsedPercentModified(config.getMinFreeDiskSpaceToRunJobs());
        int activeTaskWeight = config.getActiveTaskWeight();
        int diskUsedWeight = config.getDiskUsedWeight();
        // Assemble the score as a combination of the mean active tasks and the disk used
        double exponent = 2.5;
        double score = activeTaskWeight * Math.pow(meanActive, exponent);
        double diskPercentPowered = Math.pow(diskUsedPercentModified, exponent);
        score += diskUsedWeight * diskPercentPowered;
        // If host is very full, make sure to give the host a big score
        score = Math.max(score, (activeTaskWeight + diskUsedWeight) * diskPercentPowered);
        return new HostScore(meanActive, diskUsedPercentModified, score);
    }

    /**
     * Is this host's load significantly different from the rest of the cluster?
     *
     * @param hostID    The host to check
     * @param diskSpace Whether to consider load based on disk space (as opposed to number of tasks)
     * @param high      Whether to look for heavy load as opposed to light load
     * @return True if the host has the specified level of load
     */
    protected boolean isExtremeHost(@Nullable String hostID, boolean diskSpace, boolean high) {
        aggregateStatisticsLock.lock();
        try {
            if ((hostID == null) || (cachedHostScores == null) || !cachedHostScores.containsKey(hostID) ||
                cachedHostScores.isEmpty()) {
                return false;
            }
            double clusterAverage = 0;
            for (HostScore score : cachedHostScores.values()) {
                clusterAverage += score.getScoreValue(diskSpace);
            }
            clusterAverage /= cachedHostScores.size(); // Nonzero as we check if cachedHostScores.isEmpty first
            double hostValue = cachedHostScores.get(hostID).getScoreValue(diskSpace);
            return (high && (hostValue > (clusterAverage * config.getExtremeHostRatio()))) ||
                   (!high && (hostValue < (clusterAverage / config.getExtremeHostRatio())));
        } finally {
            aggregateStatisticsLock.unlock();
        }

    }


    private MoveAssignmentList moveTasksOntoHost(JobTaskItemByHostMap tasksByHost,
                                                 int maxPerHost,
                                                 int numToMove,
                                                 long maxBytesToMove,
                                                 String pullHost) {
        MoveAssignmentList rv = new MoveAssignmentList(spawn, taskSizer);
        Collection<JobKey> alreadyMoved = new HashSet<>();
        Iterator<String> otherHosts = tasksByHost.getHostIterator(true);

        if (isExtremeHost(pullHost, true, true)) {
            return rv;
        }
        if (isExtremeHost(pullHost, false, true)) {
            numToMove = Math.max(1, numToMove / 2); // Move fewer tasks onto a host if it's already doing a lot of work
        }
        HostState pullHostState = hostManager.getHostState(pullHost);
        if (pullHostState == null) {
            return rv;
        }
        while (otherHosts.hasNext() && (rv.size() < numToMove) && (tasksByHost.get(pullHost).size() < maxPerHost)) {
            String pushHost = otherHosts.next();
            if ((pushHost == null) || pushHost.equals(pullHost)) {
                continue;
            }
            Collection<JobTaskItem> pushHostItems = new ArrayList<>(tasksByHost.get(pushHost));
            if (pushHostItems.size() < maxPerHost) {
                break;
            }
            for (JobTaskItem item : pushHostItems) {
                JobKey jobKey = item.getTask().getJobKey();
                Job job = spawn.getJob(jobKey);
                if ((job == null) || !pullHostState.getMinionTypes().contains(job.getMinionType())) {
                    continue;
                }
                long trueSizeBytes = taskSizer.estimateTrueSize(item.getTask());
                if (pullHostState.hasLive(item.getTask().getJobKey()) ||
                    ((maxBytesToMove > 0) && (trueSizeBytes > maxBytesToMove)) ||
                    !isTaskSpreadOutAcrossAd(hostManager.getHostLocationForHost(pushHost),
                                             hostManager.getHostLocationForHost(pullHost),
                                             item.getTask())) {
                    continue;
                }

                if (!alreadyMoved.contains(jobKey) && tasksByHost.moveTask(item, pushHost, pullHost)) {
                    rv.add(new JobTaskMoveAssignment(item.getTask().getJobKey(), pushHost, pullHost, false, false));
                    alreadyMoved.add(item.getTask().getJobKey());
                    maxBytesToMove -= trueSizeBytes;
                }

                if (rv.size() >= numToMove) {
                    break;
                }
            }
        }
        return rv;
    }

    /* Pull tasks off the given host and move them elsewhere, obeying the maxPerHost and maxBytesToMove limits */

    private MoveAssignmentList moveTasksOffHost(JobTaskItemByHostMap tasksByHost,
                                                int maxPerHost,
                                                int numToMove,
                                                long maxBytesToMove,
                                                String pushHost) {
        if (log.isDebugEnabled()) {
            log.debug("received move assignment maxPerHost:{} numToMove:{} pushHost:{}", maxPerHost, numToMove,
                      pushHost);
        }
        MoveAssignmentList rv = new MoveAssignmentList(spawn, taskSizer);
        Collection<JobKey> alreadyMoved = new HashSet<>();
        Iterator<String> otherHosts = tasksByHost.getHostIterator(true);

        while (otherHosts.hasNext() && (rv.size() < numToMove)) {
            String pullHost = otherHosts.next();
            if (pushHost.equals(pullHost)) {
                continue;
            }
            HostState pullHostState = hostManager.getHostState(pullHost);
            Iterator<JobTaskItem> itemIterator = new ArrayList<>(tasksByHost.get(pushHost)).iterator();
            while (itemIterator.hasNext() && (rv.size() < numToMove) &&
                   (tasksByHost.get(pullHost).size() < maxPerHost)) {
                JobTaskItem nextTaskItem = itemIterator.next();
                long trueSizeBytes = taskSizer.estimateTrueSize(nextTaskItem.getTask());
                JobKey jobKey = nextTaskItem.getTask().getJobKey();
                Job job = spawn.getJob(jobKey);
                if ((job == null) || !pullHostState.getMinionTypes().contains(job.getMinionType())) {
                    continue;
                }
                // Reject the move if the target host is heavily loaded, already has a copy of the task, or the task
                // is too large
                // or if the move results in uneven spread of tasks and replicas across ADs
                if (isExtremeHost(pullHost, true, true) || pullHostState.hasLive(jobKey) ||
                    ((maxBytesToMove > 0) && (trueSizeBytes > maxBytesToMove)) ||
                    !isTaskSpreadOutAcrossAd(hostManager.getHostLocationForHost(pushHost),
                                             hostManager.getHostLocationForHost(pullHost),
                                             nextTaskItem.getTask())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Unable to move task to host {} fullDisk={} alreadyLive={} byteCount={}>{} {}",
                                  pullHost, isExtremeHost(pullHost, true, true), pullHostState.hasLive(jobKey),
                                  trueSizeBytes, maxBytesToMove, trueSizeBytes > maxBytesToMove);
                    }
                    continue;
                }

                if (!alreadyMoved.contains(jobKey) && (pullHost != null) &&
                    tasksByHost.moveTask(nextTaskItem, pushHost, pullHost)) {
                    rv.add(new JobTaskMoveAssignment(nextTaskItem.getTask().getJobKey(), pushHost, pullHost, false,
                                                     false));
                    alreadyMoved.add(nextTaskItem.getTask().getJobKey());
                    maxBytesToMove -= trueSizeBytes;
                }
            }
        }
        return rv;
    }
    /* Push tasks onto the given host, obeying the maxPerHost and maxBytesToMove limits */

    /* Count the number of tasks that live on each host */
    private JobTaskItemByHostMap generateTaskCountByHost(List<HostState> hosts, Iterable<JobTask> tasks) {
        JobTaskItemByHostMap rv = new JobTaskItemByHostMap(this, hosts, config.getTasksMovedPerUnspecifiedHost(),
                                                           config.getTasksMovedPerUnspecifiedHost());
        for (JobTask task : tasks) {
            rv.addLiveAndReplicasForTask(task);
        }
        return rv;
    }

    /**
     * Pushes one task from each heavyHost on to the lightHost
     * @param host lightHost to receive tasks
     * @param heavyHosts list of heavyHosts
     * @return list of valid JobTaskMoveAssignment(s)
     */
    private List<JobTaskMoveAssignment> pushTasksOntoHost(HostState host, Collection<HostState> heavyHosts) {
        int moveLimit = config.getTasksMovedFullRebalance();
        MoveAssignmentList moveAssignments = new MoveAssignmentList(spawn, taskSizer);

        List<HostState> lightHostList = Arrays.asList(host);
        // byteLimitFactor = 1
        long byteLimit = config.getBytesMovedFullRebalance();

        for (HostState heavyHost : heavyHosts) {
            List<JobTaskMoveAssignment> assignments =
                    getMoveAssignments(heavyHost, lightHostList, true, byteLimit, moveLimit, true);

            // Add only one assignment from this heavyHost
            for (JobTaskMoveAssignment assignment : assignments) {
                // Add the assignment only if it does not move replicas of the same task to the same target host
                if (moveAssignments.add(assignment)) {
                    // Update byteLimit with available byte limit
                    byteLimit -= moveAssignments.getBytesUsed();
                    markRecentlyReplicatedTo(Arrays.asList(assignment));
                    break;
                }
            }

            if (moveAssignments.size() >= moveLimit) {
                break;
            }
        }

        List<JobTaskMoveAssignment> rv = purgeMisplacedTasks(host, moveLimit);
        rv.addAll(moveAssignments.getList());
        return rv;
    }

    /* For each active job, ensure that the given host has a fair share of tasks from that job */
    private Collection<JobTaskMoveAssignment> balanceActiveJobsOnHost(HostState host, List<HostState> hosts) {
        int totalTasksToMove = config.getTasksMovedFullRebalance();
        long totalBytesToMove = config.getBytesMovedFullRebalance();
        Set<String> activeJobs = getActiveJobIds();
        List<JobTaskMoveAssignment> rv = purgeMisplacedTasks(host, 1);
        String hostID = host.getHostUuid();
        for (String jobID : activeJobs) {
            // Maybe
            spawn.acquireJobLock();
            try {
                Job job = spawn.getJob(jobID);
                if (job != null) {
                    JobTaskItemByHostMap tasksByHost =
                            new JobTaskItemByHostMap(this, hosts, config.getTasksMovedPerUnspecifiedHost(),
                                                     config.getTasksMovedPerUnspecifiedHost());
                    for (JobTask task : job.getCopyOfTasks()) {
                        tasksByHost.addLiveAndReplicasForTask(task);
                    }
                    int maxPerHost = maxTasksPerHost(job, hosts.size());
                    int numExistingTasks = tasksByHost.get(hostID).size();
                    if ((tasksByHost.findLeastTasksOnHost() >= (maxPerHost - 1)) ||
                        (tasksByHost.findMostTasksOnHost() <= maxPerHost)) {
                        continue;
                    }
                    boolean pushFrom = (numExistingTasks > maxPerHost) || ((numExistingTasks == maxPerHost) &&
                                                                           (tasksByHost.findLeastTasksOnHost() <
                                                                            (maxPerHost - 1)));
                    if (totalTasksToMove > 0) {
                        MoveAssignmentList assignments = pushFrom ?
                                                         moveTasksOffHost(tasksByHost, maxPerHost, 1, totalBytesToMove,
                                                                          host.getHostUuid())
                                                                  :
                                                         moveTasksOntoHost(tasksByHost, maxPerHost, 1, totalBytesToMove,
                                                                           host.getHostUuid());
                        rv.addAll(assignments.getList());
                        totalTasksToMove -= assignments.size();
                        totalBytesToMove -= assignments.getBytesUsed();
                    } else {
                        break;
                    }
                }
            } finally {
                spawn.releaseJobLock();
            }
        }
        return rv;
    }

    /**
     * Prune a tentative list of task reassignments, removing illegal moves or moves to overburdened hosts
     *
     * @param candidateAssignments The initial list of assignments
     * @return A list of assignments with illogical moves removed
     */
    private List<JobTaskMoveAssignment> pruneTaskReassignments(Iterable<JobTaskMoveAssignment> candidateAssignments) {
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        Map<String, Boolean> snapshot = new HashMap<>(recentlyBalancedHosts.asMap());
        for (JobTaskMoveAssignment assignment : candidateAssignments) {
            String newHostID = assignment.getTargetUUID();
            // Task should be deleted
            if(assignment.delete()) {
                rv.add(assignment);
            } else {
                // Target host is null, assignment cannot be executed
                if (newHostID == null) {
                    continue;
                }
                JobKey jobKey = assignment.getJobKey();
                String jobID = (jobKey == null) ? null : jobKey.getJobUuid();
                if (isExtremeHost(newHostID, true, true)) {
                    log.warn("[spawn.balancer] decided not to move task from job {} to host {} " +
                             "because it is already heavily loaded", jobID, newHostID);
                    continue;
                }
                HostState newHost = hostManager.getHostState(newHostID);
                if ((newHost == null) || newHost.hasLive(jobKey) || !canReceiveNewTasks(newHost)) {
                    log.warn("[spawn.balancer] decided not to move task from job {} to host {} " +
                             "because it cannot receive the new task", jobID, newHostID);
                    continue;
                }
                if (snapshot.containsKey(newHostID)) {
                    log.warn("[spawn.balancer] decided not to move task from job {} to host {} " +
                             "because it already received a different task recently", jobID, newHostID);
                    continue;
                }
                rv.add(assignment);
                recentlyBalancedHosts.put(newHostID, true);
            }
        }
        return rv;
    }

    private static List<JobTaskMoveAssignment> removeDuplicateAssignments(Iterable<JobTaskMoveAssignment>
                                                                                  candidateAssignments) {
        Collection<JobKey> movedTasks = new HashSet<>();
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        for (JobTaskMoveAssignment assignment : candidateAssignments) {
            JobKey jobKey = assignment.getJobKey();
            if (!movedTasks.contains(jobKey)) {
                rv.add(assignment);
                movedTasks.add(jobKey);
            }
        }
        return rv;
    }

    /**
     * Assuming a job has n tasks, each with R replicas, and there are m hosts available, each host should have no more
     * than n*(1+R)/m, rounded up.
     */
    private static int maxTasksPerHost(Job job, int numHosts) {
        if (job == null) {
            return 0;
        }
        numHosts = Math.max(1, numHosts);
        return (int) Math.ceil((double) (job.getTaskCount() * (1 + job.getReplicas())) / numHosts);
    }

    private static int getWeightedElementIndex(int numItems, RebalanceWeight weight) {
        switch (weight) {
            case LIGHT:
                return 0;
            case MEDIUM:
                return numItems / 2;
            case HEAVY:
                return numItems - 1;
            default:
                throw new IllegalArgumentException("unknown weight type " + weight);
        }
    }

    /**
     * Returns a collection of tasks that are valid to move from {@code fromHostUuid} to {@code toHostUuid},
     * which will contain between 0 and {@code numTasks} tasks.
     * <p>
     * Validity checks include:
     * - task must be non-null and idle or queued
     * - tasks must be supposed to live on the fromHost
     * - if obeyDontAutobalanceMe is false, job.getDontAutoBalanceMe must be false
     * - Move must result in task and replicas being all on different minAds or having copies in every minAd
     *
     * @param fromHostUuid          Host to move task from
     * @param toHostUuid            Host to move task to
     * @param numTasks              Maximum number of tasks to return
     * @param obeyDontAutobalanceMe set to true for filesystem-okay host failure
     */
    // NOTE: keeping this code around for potential future improvements to spawnbalancer.
    @SuppressWarnings("FloatingPointEquality")
    public @Nonnull Collection<JobTask> getTasksToMove(String fromHostUuid, String toHostUuid,
                                                       int numTasks, boolean obeyDontAutobalanceMe) {
        // loop through all tasks
        // cache the numScore lowest scores that can validly move
        // if we find numScore that have minscore and can validly move, just return them immediately

        // primaryAd             The primary AD in which to ensure spread
        AvailabilityDomain primaryAd = hostManager.getHostLocationSummary().getPriorityLevel();
        PriorityQueue<WithScore<JobTask>> sortedTasks = new PriorityQueue<>(numTasks, taskComparator);
        HostState fromHost = hostManager.getHostState(fromHostUuid);
        HostLocation fromHostLocation = fromHost.getHostLocation();
        HostLocation toHostLocation = hostManager.getHostState(toHostUuid).getHostLocation();
        for (JobKey jobKey : fromHost.allJobKeys()) {
            JobTask task = spawn.getTask(jobKey);
            Job job = spawn.getJob(jobKey);

            if ((job != null) && SpawnBalancer.isInMovableState(task) &&
                (fromHostUuid.equals(task.getHostUUID()) || task.hasReplicaOnHost(fromHostUuid))) {
                if (obeyDontAutobalanceMe && job.getDontAutoBalanceMe()) {
                    // obeyDontAutobalanceMe is set to false when spawn is doing a filesystem-okay host failure.
                    // In this case, spawn needs to move the task even if the job owner specified no swapping,
                    // because the box is likely to be ailing/scheduled for decommission.
                    // All rebalancing actions use obeyDontAutobalanceMe=true and will conform to the job owner's
                    // wishes.
                    continue;
                }
                if (!this.isTaskSpreadOutAcrossAd(fromHostLocation, toHostLocation, task)) {
                    // only allow tasks to be moved that are correctly spread out
                    continue;
                }

                double taskScore = this.calculateTaskScore(fromHostLocation, toHostLocation, task);
                // fixme: null pointer exception
                double highestScore = sortedTasks.peek().score;
                if (sortedTasks.isEmpty() || (highestScore > taskScore)) {
                    // add to queue if it's empty or this task has a lower score than the task with the largest score
                    sortedTasks.add(new WithScore<>(task, (double) taskScore));
                } else if ((sortedTasks.size() == numTasks) && (highestScore <= (double) primaryAd.score)) {
                    // exit loop and return tasks if we have enough tasks and the largest score equals the min score
                    break;
                }
                if (sortedTasks.size() > numTasks) {
                    // remove largest score after pushing on a lower score
                    sortedTasks.poll();
                }
            }
        }
        // this could contain 0 - numTasks tasks.
        return sortedTasks.stream().map(WithScore::getElement).collect(Collectors.toList());
    }

    /**
     * Calculate the availability-domain-score of a task moving from one host to another.
     * A lower score is a better move to make, because it more distant.
     */
    private double calculateTaskScore(HostLocation fromHostLocation, HostLocation toHostLocation, JobTask task) {
        // get average distance score for all tasks host locations (except any on the fromHostLocation)
        return task.getAllTaskHosts()
                   .stream()
                   .map(h -> hostManager.getHostState(h).getHostLocation())
                   .filter(hl -> !hl.equals(fromHostLocation))
                   .mapToInt(toHostLocation::assignScoreByHostLocation)
                   .average().orElse(0D);
    }

    /**
     * Returns true if moving a replica for {@code task} from {@code fromHostLocation} to
     * {@code toHostLocation} is correctly spread out so that either all replicas will be on
     * different min ads, or there is a replica on every min ad.
     */
    public boolean isTaskSpreadOutAcrossAd(@Nullable HostLocation fromHostLocation,
                                           @Nullable HostLocation toHostLocation, JobTask task) {
        AvailabilityDomain primaryAd = hostManager.getHostLocationSummary().getPriorityLevel();

        if(primaryAd == AvailabilityDomain.NONE) {
            return true;
        }

        int minAdCardinality = hostManager.getHostLocationSummary().getMinCardinality(primaryAd);
        List<HostLocation> hostLocations = task.getAllTaskHosts()
                                               .stream()
                                               .map(h -> hostManager.getHostLocationForHost(h))
                                               .collect(Collectors.toList());
        // IF this is a MOVE
        // remove the FIRST instance of the location we're moving FROM
        if(fromHostLocation != null && toHostLocation != null) {
            hostLocations.remove(fromHostLocation);
            hostLocations.add(toHostLocation);
        }
        Set<String> minAdsUsed = new HashSet<>(minAdCardinality);
        boolean allReplicasOnDifferentAd = true;
        for (HostLocation location : hostLocations) {
            boolean isNew = minAdsUsed.add(location.getPriorityAd(primaryAd));
            // if any min ad was already in minAdsUsed, then all replicas are not on the same min ad.
            if (!isNew) {
                allReplicasOnDifferentAd = false;
            }
            // if all min ads are used, then the move is sufficiently spread out
            if (minAdsUsed.size() == minAdCardinality) {
                return true;
            }
        }
        // if not all min ads are used, then the move is spread out if all replicas are on different min ads.
        return allReplicasOnDifferentAd;
    }

}

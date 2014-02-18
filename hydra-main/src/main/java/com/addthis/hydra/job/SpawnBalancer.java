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
package com.addthis.hydra.job;

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
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.hydra.job.chores.JobTaskMoveAssignment;
import com.addthis.hydra.job.mq.CommandTaskStop;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * A class in charge of balancing load among spawn's hosts.
 * General assumptions:
 * The boxes are not so asymmetrical that running a job on three boxes is slower than running it on a single box.
 * Jobs run faster when they have as few tasks grouped together on individual boxes as possible.
 * <p/>
 * Groups of tasks from the same job living together on machines are termed "job siblings."
 * Motivation: they come from the same source and compete with each other for resources.
 */
public class SpawnBalancer implements Codec.Codable {

    private final Spawn spawn;
    private static final Logger log = LoggerFactory.getLogger(SpawnBalancer.class);

    @Codec.Set(codable = true)
    private SpawnBalancerConfig config = new SpawnBalancerConfig();

    // How often to update aggregate host statistics
    private static final long AGGREGATE_STAT_UPDATE_INTERVAL = Parameter.intValue("spawnbalance.stat.update", 15 * 1000);
    private static long lastAggregateStatUpdateTime = 0;

    private ConcurrentHashMap<String, HostScore> cachedHostScores = new ConcurrentHashMap<>();
    private Set<String> activeJobIDs;
    private ReentrantLock aggregateStatisticsLock = new ReentrantLock();

    private AtomicBoolean autobalanceStarted = new AtomicBoolean(false);

    private Cache<String, Boolean> recentlyAutobalancedJobs = CacheBuilder.newBuilder().expireAfterWrite(
            Parameter.intValue("spawnbalance.job.autobalance.interval.mins", 60 * 12), TimeUnit.MINUTES
    ).build();

    private Cache<String, Boolean> recentlyBalancedHosts = CacheBuilder.newBuilder().expireAfterWrite(
            Parameter.intValue("spawnbalance.host.balance.interval.mins", 3), TimeUnit.MINUTES
    ).build();

    private Cache<String, Boolean> recentlyReplicatedToHosts = CacheBuilder.newBuilder().expireAfterWrite(
            Parameter.intValue("spawnbalance.host.replicate.interval.mins", 15), TimeUnit.MINUTES
    ).build();

    private final SpawnBalancerTaskSizer taskSizer;

    private final Comparator<HostAndScore> hostAndScoreComparator = new Comparator<HostAndScore>() {
        @Override
        public int compare(HostAndScore firstHAS, HostAndScore secondHAS) {
            return Double.compare(firstHAS.score, secondHAS.score);
        }
    };

    private final Comparator<HostState> hostStateScoreComparator = new Comparator<HostState>() {
        @Override
        public int compare(HostState hostState, HostState hostState1) {
            return Double.compare(getHostScoreCached(hostState.getHostUuid()), getHostScoreCached(hostState1.getHostUuid()));
        }
    };

    private final Comparator<Job> jobAverageTaskSizeComparator = new Comparator<Job>() {
        @Override
        public int compare(Job job, Job job1) {
            if (job == null || job1 == null) {
                return 0;
            } else {
                return Double.compare(job.calcAverageTaskSizeBytes(), job1.calcAverageTaskSizeBytes());
            }
        }
    };

    private final Comparator<HostState> hostStateReplicationSuitabilityComparator = new Comparator<HostState>() {
        @Override
        public int compare(HostState hostState, HostState hostState1) {
            boolean recentlyReplicatedTo = recentlyReplicatedToHosts.getIfPresent(hostState.getHostUuid()) != null;
            boolean recentlyReplicatedTo1 = recentlyReplicatedToHosts.getIfPresent(hostState1.getHostUuid()) != null;
            if (recentlyReplicatedTo != recentlyReplicatedTo1) {
                return recentlyReplicatedTo ? 1 : -1;
            }
            return -Double.compare(getAvailDiskBytes(hostState), getAvailDiskBytes(hostState1));
        }
    };

    public SpawnBalancer(Spawn spawn) {
        this.spawn = spawn;
        Timer statUpdateTimer = new Timer(true);
        statUpdateTimer.scheduleAtFixedRate(new AggregateStatUpdaterTask(), AGGREGATE_STAT_UPDATE_INTERVAL, AGGREGATE_STAT_UPDATE_INTERVAL);
        this.taskSizer = new SpawnBalancerTaskSizer(spawn);
    }

    public void startTaskSizePolling() {
        taskSizer.startPolling();
    }

    /**
     * Is this job non-null and has it run recently?
     */
    private boolean isWellFormedAndActiveJob(Job job) {
        long earliestActiveTime = JitterClock.globalTime() - config.getActiveTaskMilliCutoff();
        return job != null && job.getStartTime() != null && job.getStartTime() > earliestActiveTime;
    }

    /**
     * Makes the specified number of tasks for the given job ID, and assigns each task to a suitable host.
     *
     * @param jobID     the job in question
     * @param taskCount how many tasks should be created
     * @param hosts     a current set of hosts
     * @return a list of tasks with HostID set.
     */
    protected List<JobTask> generateAssignedTasksForNewJob(String jobID, int taskCount, List<HostState> hosts) throws Exception {
        List<JobTask> tasks = generateUnassignedTaskList(jobID, taskCount);
        Map<JobTask, String> hostAssignments = assignTasksFromSingleJobToHosts(tasks, generateHostStateScoreMap(hosts, null));
        List<JobTask> rv = new ArrayList<>(tasks.size());
        for (Entry<JobTask, String> entry : hostAssignments.entrySet()) {
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
    private List<JobTask> generateUnassignedTaskList(String jobID, int taskCount) {
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
     * Take a list of tasks belonging to a single job and find the best hosts to house these tasks.
     * A map of host scores is passed as a parameter so that it can be persisted across multiple calls to this function
     * by, for example, assignTasksFromMultipleJobsToHosts.
     */
    private Map<JobTask, String> assignTasksFromSingleJobToHosts(List<JobTask> tasks, Map<HostState, Double> storedHostScores) {
        if (tasks == null || tasks.isEmpty()) {
            return new HashMap<>();
        }
        // Make a heap of hosts based on the storedHostScores, from lightest to heaviest.
        PriorityQueue<HostAndScore> hostScores = new PriorityQueue<>(1 + storedHostScores.size(), hostAndScoreComparator);
        for (HostState host : storedHostScores.keySet()) {
            if (canReceiveNewTasks(host, false)) {
                hostScores.add(new HostAndScore(host, storedHostScores.get(host)));
            }
        }
        if (hostScores.isEmpty()) {
            log.warn("[spawn.balancer] found no hosts eligible to receive new tasks");
            throw new RuntimeException("no eligible hosts for new task");
        }
        // Make a list of hosts as big as the list of tasks. This list may repeat hosts if it is necessary to do so.
        List<String> hostsToAssign = new ArrayList<>(tasks.size());
        for (JobTask task : tasks) {
            if (task == null) {
                continue;
            }
            // Pick the lightest host.
            HostAndScore h = hostScores.poll();
            HostState host = h.host;
            hostsToAssign.add(host.getHostUuid());
            // Then moderately weight that host in the heap so we won't pick it again immediately.
            hostScores.add(new HostAndScore(host, h.score + config.getSiblingWeight()));
            // Lightly weight that host in storedHostScores so we won't pick the same hosts over and over if we call this method repeatedly
            storedHostScores.put(host, h.score + config.getActiveTaskWeight());
        }
        return pairTasksAndHosts(tasks, hostsToAssign);
    }

    /**
     * Given a list of tasks and a list of potential hosts of the same size, pair them up.
     * Keep tasks on their existing hosts as much as possible.
     */
    private Map<JobTask, String> pairTasksAndHosts(List<JobTask> tasks, List<String> hosts) {
        if (tasks == null || hosts == null || tasks.size() != hosts.size()) {
            log.warn("[spawn.balancer] invalid call to pairTasksAndHosts: tasks=" + tasks + " hosts=" + hosts);
            return new HashMap<>(0);
        }
        Map<JobTask, String> rv = new HashMap<>(tasks.size());
        // For any task already living on a host in our hosts list, keep that task where it is.
        List<JobTask> unassignedTasks = new ArrayList<>();
        String jobID = tasks.get(0).getJobUUID();
        for (JobTask task : tasks) {
            if (!(task.getJobUUID().equals(jobID))) {
                throw new RuntimeException("Illegal call to assignTasksFromSingleJobToHosts: not all tasks came from the same job");
            }
            String hostID = task.getHostUUID();
            if (hostID != null && hosts.contains(hostID)) {
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

    /**
     * Takes any arbitrary list of tasks and finds the best hosts to house all of them.
     *
     * @param tasks The tasks to assign to hosts
     * @param hosts All available hosts in the cluster
     * @return A map describing where to send each task
     */
    public Map<JobTask, String> assignTasksFromMultipleJobsToHosts(List<JobTask> tasks, List<HostState> hosts) {
        // Populate a map grouping the tasks by job ID.
        HashMap<String, List<JobTask>> tasksByJobID = new HashMap<>();
        for (JobTask task : tasks) {
            Job job;
            if (task.getJobUUID() != null && (job = spawn.getJob(task.getJobUUID())) != null) {
                List<JobTask> taskList = tasksByJobID.get(job.getId()) != null ? tasksByJobID.get(job.getId()) : new ArrayList<JobTask>();
                taskList.add(task);
                tasksByJobID.put(job.getId(), taskList);
            }
        }
        // This map of hosts to scores will be updated by every call to assignTasksFromSingleJobToHosts.
        Map<HostState, Double> hostScoreMap = generateHostStateScoreMap(hosts, null);
        // This map stores where to send each task.
        HashMap<JobTask, String> hostAssignments = new HashMap<>(tasks.size());
        for (Entry<String, List<JobTask>> entry : tasksByJobID.entrySet()) {
            Map<JobTask, String> singleHostAssignments = assignTasksFromSingleJobToHosts(entry.getValue(), hostScoreMap);
            hostAssignments.putAll(singleHostAssignments);
        }
        return hostAssignments;
    }

    /**
     * Advises Spawn how to reallocate a job, sending some tasks to different hosts
     *
     * @param job         The job being reallocated
     * @param tasksToMove The number of tasks to move. If <= 0, use the default.
     * @param hosts       The available target hosts
     * @return a map assigning a good host for each jobtask
     */
    protected List<JobTaskMoveAssignment> getAssignmentsForJobReallocation(Job job, int tasksToMove, List<HostState> hosts) {
        int maxTasksToMove = tasksToMove > 0 ? tasksToMove : config.getTasksMovedFullRebalance();
        List<JobTaskMoveAssignment> candidateAssignments = new ArrayList<>();
        // Count the number of tasks per host
        JobTaskItemByHostMap tasksByHost = generateTaskCountByHost(hosts, job.getCopyOfTasks());
        // Find the max number of tasks each host should have
        int maxPerHost = maxTasksPerHost(job, tasksByHost.size());
        if (log.isDebugEnabled()) {
            log.debug("Rebalancing job: " + job.getId() + " maxTasksToMove=" + maxTasksToMove + " maxPerHost=" + maxPerHost);
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
                log.debug("hostsSorted.size=" + hostsSorted.size() +
                          " hostWithMost:" + hostWithMost + " hostWithLeast:" + hostWithLeast +
                          " mostTasksOnHost: " + mostTasksOnHost + " leastTasksOnHost: " + leastTasksOnHost);
            }

            // If any host has more than the max number, move some tasks off that host
            if (mostTasksOnHost > maxPerHost) {
                moves = moveTasksOffHost(tasksByHost, maxPerHost, 1, -1, hostWithMost);
            } else if (leastTasksOnHost < maxPerHost - 1) { // If a host has significantly fewer than the max number, move some tasks onto that host
                moves = moveTasksOntoHost(tasksByHost, maxPerHost, 1, -1, hostWithLeast);
            } else if (isExtremeHost) { // If a host with many tasks is heavily weighted, move a task off that host
                moves = moveTasksOffHost(tasksByHost, maxPerHost, 1, -1, hostWithMost);
            }
            if (moves == null || moves.isEmpty()) {
                break;
            } else {
                candidateAssignments.addAll(moves);
            }
        }
        if (candidateAssignments.size() > maxTasksToMove) {
            candidateAssignments = candidateAssignments.subList(0, maxTasksToMove);
        }
        candidateAssignments = removeDuplicateAssignments(candidateAssignments);
        return pruneTaskReassignments(candidateAssignments);
    }

    private List<JobTaskMoveAssignment> removeDuplicateAssignments(List<JobTaskMoveAssignment> candidateAssignments) {
        HashSet<JobKey> movedTasks = new HashSet<>();
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        for (JobTaskMoveAssignment assignment : candidateAssignments) {
            JobKey jobKey = assignment.getTask().getJobKey();
            if (!movedTasks.contains(jobKey)) {
                rv.add(assignment);
                movedTasks.add(jobKey);
            }
        }
        return rv;
    }

    /* Pull tasks off the given host and move them elsewhere, obeying the maxPerHost and maxBytesToMove limits */
    private MoveAssignmentList moveTasksOffHost(JobTaskItemByHostMap tasksByHost, int maxPerHost, int numToMove, long maxBytesToMove, String pushHost) {
        if (log.isDebugEnabled()) {
            log.debug("received move assignment maxPerHost:" + maxPerHost + " numToMove:" + numToMove + " pushHost:" + pushHost);
        }
        MoveAssignmentList rv = new MoveAssignmentList();
        Set<JobKey> alreadyMoved = new HashSet<>();
        Iterator<String> otherHosts = tasksByHost.getHostIterator(true);
        while (otherHosts.hasNext() && rv.size() < numToMove) {
            String pullHost = otherHosts.next();
            if (pushHost.equals(pullHost)) {
                continue;
            }
            HostState pullHostState = spawn.getHostState(pullHost);
            Iterator<JobTaskItem> itemIterator = new ArrayList<>(tasksByHost.get(pushHost)).iterator();
            while (itemIterator.hasNext() && rv.size() < numToMove && tasksByHost.get(pullHost).size() < maxPerHost) {
                JobTaskItem nextTaskItem = itemIterator.next();
                long trueSizeBytes = taskSizer.estimateTrueSize(nextTaskItem.getTask());
                JobKey jobKey = nextTaskItem.getTask().getJobKey();
                // Reject the move if the target host is heavily loaded, already has a copy of the task, or the task is too large
                if (isExtremeHost(pullHost, true, true) || pullHostState.hasLive(jobKey) || (maxBytesToMove > 0 && trueSizeBytes > maxBytesToMove)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Unable to move task to host " + pullHost + " fullDisk=" + isExtremeHost(pullHost, true, true)
                                 + " alreadyLive=" + pullHostState.hasLive(jobKey)
                                 + " byteCount=" + trueSizeBytes + ">" + maxBytesToMove + " "
                                 + (trueSizeBytes > maxBytesToMove));
                    }
                    continue;
                }
                if (!alreadyMoved.contains(jobKey) && pullHost != null && tasksByHost.moveTask(nextTaskItem, pushHost, pullHost)) {
                    rv.add(new JobTaskMoveAssignment(nextTaskItem.getTask(), pushHost, pullHost, false, false, false));
                    alreadyMoved.add(nextTaskItem.getTask().getJobKey());
                    maxBytesToMove -= trueSizeBytes;
                }
            }
        }
        return rv;
    }

    /* Push tasks onto the given host, obeying the maxPerHost and maxBytesToMove limits */
    private MoveAssignmentList moveTasksOntoHost(JobTaskItemByHostMap tasksByHost, int maxPerHost, int numToMove, long maxBytesToMove, String pullHost) {
        MoveAssignmentList rv = new MoveAssignmentList();
        Set<JobKey> alreadyMoved = new HashSet<>();
        Iterator<String> otherHosts = tasksByHost.getHostIterator(false);
        if (isExtremeHost(pullHost, true, true)) {
            return rv;
        }
        if (isExtremeHost(pullHost, false, true)) {
            numToMove = Math.max(1, numToMove /= 2); // Move fewer tasks onto a host if it's already doing a lot of work
        }
        while (otherHosts.hasNext() && rv.size() < numToMove && tasksByHost.get(pullHost).size() < maxPerHost) {
            String pushHost = otherHosts.next();
            if (pushHost == null || pushHost.equals(pullHost)) {
                continue;
            }
            HostState pullHostState = spawn.getHostState(pullHost);
            List<JobTaskItem> pushHostItems = new ArrayList<>(tasksByHost.get(pushHost));
            if (pushHostItems.size() < maxPerHost) {
                break;
            }
            for (JobTaskItem item : pushHostItems) {
                JobKey jobKey = item.getTask().getJobKey();
                long trueSizeBytes = taskSizer.estimateTrueSize(item.getTask());
                if (pullHostState.hasLive(item.getTask().getJobKey()) || (maxBytesToMove > 0 && trueSizeBytes > maxBytesToMove)) {
                    continue;
                }
                if (!alreadyMoved.contains(jobKey) && tasksByHost.moveTask(item, pushHost, pullHost)) {
                    rv.add(new JobTaskMoveAssignment(item.getTask(), pushHost, pullHost, false, false, false));
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

    /* Count the number of tasks that live on each host */
    private JobTaskItemByHostMap generateTaskCountByHost(List<HostState> hosts, List<JobTask> tasks) {
        JobTaskItemByHostMap rv = new JobTaskItemByHostMap(hosts, config.getTasksMovedPerUnspecifiedHost(), config.getTasksMovedPerUnspecifiedHost());
        for (JobTask task : tasks) {
            rv.addLiveAndReplicasForTask(task);
        }
        return rv;
    }

    /**
     * A class for maintaining a count of live/replica tasks by host
     */
    private class JobTaskItemByHostMap extends HashMap<String, Set<JobTaskItem>> {

        private final HashMap<String, Integer> pushedTaskCounts;
        private final HashMap<String, Integer> pulledTaskCounts;
        private List<String> hostsSorted = null;
        private final int maxPulledFromHost;
        private final int maxPushedToHost;

        public JobTaskItemByHostMap(List<HostState> hosts, int maxPulledFromHost, int maxPushedToHost) {
            this.pulledTaskCounts = new HashMap<>();
            this.pushedTaskCounts = new HashMap<>();
            this.maxPulledFromHost = maxPulledFromHost;
            this.maxPushedToHost = maxPushedToHost;
            for (HostState host : hosts) {
                if (!host.isReadOnly() && host.isUp() && !host.isDead()) {
                    put(host.getHostUuid(), new HashSet<JobTaskItem>());
                }
            }
        }

        public void add(String hostID, JobTask task) {
            if (containsKey(hostID)) {
                Set<JobTaskItem> current = get(hostID);
                current.add(new JobTaskItem(task));
            }
        }

        public void addLiveAndReplicasForTask(JobTask task) {
            add(task.getHostUUID(), task);
            List<JobTaskReplica> replicas = task.getReplicas();
            if (replicas != null) {
                for (JobTaskReplica replica : replicas) {
                    add(replica.getHostUUID(), task);
                }
            }
        }

        public boolean moveTask(JobTaskItem item, String fromHost, String toHost) {
            if (!containsKey(fromHost) || !containsKey(toHost)) {
                return false;
            } else if (!get(fromHost).contains(item) || get(toHost).contains(item)) {
                return false;
            } else if (!hasCapacity(pulledTaskCounts, fromHost, maxPulledFromHost) || !hasCapacity(pushedTaskCounts, toHost, maxPushedToHost)) {
                return false;
            } else {
                boolean success = get(fromHost).remove(item) && get(toHost).add(item);
                if (success) {
                    claimCapacity(pulledTaskCounts, fromHost);
                    claimCapacity(pushedTaskCounts, toHost);
                }
                return success;
            }
        }

        public List<String> generateHostsSorted() {
            List<String> rv = new ArrayList<String>(this.keySet());
            Collections.sort(rv, new Comparator<String>() {
                @Override
                public int compare(String s, String s1) {
                    int count = get(s).size();
                    int count1 = get(s1).size();
                    if (count != count1) {
                        return Double.compare(count, count1);
                    } else {
                        return Double.compare(getHostScoreCached(s), getHostScoreCached(s1));
                    }
                }
            });
            hostsSorted = rv;
            return new ArrayList<>(rv);
        }


        public Iterator<String> getHostIterator(boolean smallFirst) {
            if (hostsSorted == null) {
                generateHostsSorted();
            }
            ArrayList<String> copy = new ArrayList<>(hostsSorted);
            if (!smallFirst) {
                Collections.reverse(copy);
            }
            return copy.iterator();
        }

        private boolean hasCapacity(HashMap<String, Integer> map, String key, int max) {
            return !map.containsKey(key) || map.get(key) < max;
        }

        private void claimCapacity(HashMap<String, Integer> map, String key) {
            if (map.containsKey(key)) {
                map.put(key, map.get(key) + 1);
            } else {
                map.put(key, 1);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{");
            for (Entry<String, Set<JobTaskItem>> entry : this.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue().size()).append("; ");
            }
            return sb.append("}").toString();
        }

        public int findLeastTasksOnHost() {
            if (hostsSorted == null) {
                generateHostsSorted();
            }
            return get(hostsSorted.get(0)).size();
        }

        public int findMostTasksOnHost() {
            if (hostsSorted == null) {
                generateHostsSorted();
            }
            return get(hostsSorted.get(hostsSorted.size() - 1)).size();
        }
    }

    /* A class for storing a task and its live/replica status */
    private class JobTaskItem {

        private final JobTask task;

        public JobTaskItem(JobTask task) {
            this.task = task;
        }

        public JobTask getTask() {
            return task;
        }

        @Override
        public boolean equals(Object o) {
            if (o.getClass() != getClass()) {
                return false;
            }
            JobTaskItem item2 = (JobTaskItem) o;
            return task.getJobKey().matches(item2.getTask().getJobKey());
        }

        @Override
        public String toString() {
            return "JobTaskItem{task=" + task.getJobKey() + "'";
        }
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

    /**
     * Advises Spawn how to reallocate a host, pushing or pulling jobs to balance the number of tasks run by each machine.
     *
     * @param host  The particular host to consider
     * @param hosts All available hosts; should include host
     * @return a (possibly empty) map specifying some tasks and advised destinations for those tasks
     */
    protected List<JobTaskMoveAssignment> getAssignmentsToBalanceHost(HostState host, List<HostState> hosts) {
        String hostID = host.getHostUuid();
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        if (hosts == null || hosts.isEmpty()) {
            log.warn("[spawn.balancer] " + hostID + " reallocation failed: host list empty");
            return rv;
        }
        List<HostState> sortedHosts = sortHostsByDiskSpace(hosts);
        int numAlleviateHosts = (int) Math.ceil(sortedHosts.size() * config.getAlleviateHostPercentage());
        if (isExtremeHost(hostID, true, true) || getUsedDiskPercent(host) > 1 - config.getMinDiskPercentAvailToReceiveNewTasks()) {
            // Host disk is overloaded
            log.warn("[spawn.balancer] " + hostID + " categorized as overloaded host; looking for tasks to push off of it");
            List<HostState> lightHosts = sortedHosts.subList(0, numAlleviateHosts);
            rv.addAll(pushTasksOffHost(host, lightHosts, true, 1, config.getTasksMovedFullRebalance(), true));
        } else if (isExtremeHost(hostID, true, false)) {
            // Host disk is underloaded
            log.warn("[spawn.balancer] " + hostID + " categorized as underloaded host; looking for tasks to pull onto it");
            List<HostState> heavyHosts = Lists.reverse(sortedHosts.subList(sortedHosts.size() - numAlleviateHosts, sortedHosts.size()));
            pushTasksOntoDisk(host, heavyHosts);
        } else if (isExtremeHost(hostID, false, true)) {
            // Host is overworked
            log.warn("[spawn.balance] " + hostID + " categorized as overworked host; looking for tasks to push off it");
            rv.addAll(balanceActiveJobsOnHost(host, hosts));
        }
        if (rv.isEmpty()) {
            rv.addAll(balanceActiveJobsOnHost(host, hosts));
        }
        return pruneTaskReassignments(rv);
    }

    /**
     * Find suitable tasks to move off a host.
     *
     * @param host                  The hoststate to move tasks off of
     * @param obeyDontAutobalanceMe If true, obey the job parameter dontAutobalanceMe. See notes below.
     * @return A list of tasks that are suitable to be moved
     */
    private List<JobTask> findTasksToMove(HostState host, boolean obeyDontAutobalanceMe) {
        List<JobTask> rv = new ArrayList<>();
        if (host != null) {
            String hostId = host.getHostUuid();
            for (JobKey jobKey : host.allJobKeys()) {
                Job job = spawn.getJob(jobKey);
                JobTask task = spawn.getTask(jobKey);
                if (job != null && task != null && task.getState() == JobTaskState.IDLE // Only add non-null, idle tasks
                    && (hostId.equals(task.getHostUUID()) || task.hasReplicaOnHost(host.getHostUuid()))) // Only add tasks that are supposed to live on the specified host.
                {
                    if (obeyDontAutobalanceMe && job.getDontAutoBalanceMe()) {
                        // obeyDontAutobalanceMe is set to false when spawn is doing a filesystem-okay host failure.
                        // In this case, spawn needs to move the task even if the job owner specified no swapping,
                        // because the box is likely to be ailing/scheduled for decommission.
                        // All rebalancing actions use obeyDontAutobalanceMe=true and will conform to the job owner's wishes.
                        continue;
                    }
                    rv.add(task);
                }
            }
        }
        return rv;
    }

    private List<JobTaskMoveAssignment> pushTasksOntoDisk(HostState host, List<HostState> heavyHosts) {
        MoveAssignmentList moveAssignments = new MoveAssignmentList();
        for (HostState heavyHost : heavyHosts) {
            double byteLimitFactor = 1 - ((double) (moveAssignments.getBytesUsed())) / config.getBytesMovedFullRebalance();
            moveAssignments.addAll(pushTasksOffHost(heavyHost, Arrays.asList(host), true, byteLimitFactor, config.getTasksMovedFullRebalance(), true));
        }
        return moveAssignments;
    }

    public List<JobTaskMoveAssignment> pushTasksOffDiskForFilesystemOkayFailure(HostState host, int moveLimit) {
        List<HostState> hosts = spawn.listHostStatus(host.getMinionTypes());
        return pushTasksOffHost(host, hosts, false, 1, moveLimit, false);
    }

    /* Push/pull the tasks on a host to balance its disk, obeying an overall limit on the number of tasks/bytes to move */
    private List<JobTaskMoveAssignment> pushTasksOffHost(HostState host, List<HostState> otherHosts, boolean limitBytes, double byteLimitFactor, int moveLimit, boolean obeyDontAutobalanceMe) {
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        long byteLimit = (long) (byteLimitFactor * config.getBytesMovedFullRebalance());
        List<HostState> hostsSorted = sortHostsByDiskSpace(otherHosts);
        for (JobTask task : findTasksToMove(host, obeyDontAutobalanceMe)) {
            long taskTrueSize = getTaskTrueSize(task);
            if (limitBytes && taskTrueSize > byteLimit) {
                continue;
            }
            JobTaskMoveAssignment assignment = moveTask(task, host.getHostUuid(), hostsSorted);
            if (assignment != null) {
                markRecentlyReplicatedTo(assignment.getTargetUUID());
                rv.add(assignment);
                byteLimit -= taskTrueSize;
            }
            if (rv.size() >= moveLimit) {
                break;
            }
        }
        return rv;
    }

    @VisibleForTesting
    protected void markRecentlyReplicatedTo(String hostId) {
        if (hostId != null) {
            recentlyReplicatedToHosts.put(hostId, true);
        }
    }

    /**
     * Given an ordered list of hosts, move the task to a suitable host, then move that host to the end of the list
     *
     * @param task       The task to move
     * @param fromHostId The task to move the host from
     * @param otherHosts The potential target hosts
     * @return An assignment describing how to move the task
     */
    private JobTaskMoveAssignment moveTask(JobTask task, String fromHostId, List<HostState> otherHosts) {
        Iterator<HostState> hostStateIterator = otherHosts.iterator();
        String taskHost = task.getHostUUID();
        boolean live = task.getHostUUID().equals(fromHostId);
        if (!live && !task.hasReplicaOnHost(fromHostId)) {
            // Task was found somewhere it didn't belong
            return new JobTaskMoveAssignment(task, fromHostId, null, false, false, true);
        }
        while (hostStateIterator.hasNext()) {
            HostState next = hostStateIterator.next();
            String nextId = next.getHostUuid();
            if (!taskHost.equals(nextId) && !task.hasReplicaOnHost(nextId) && next.canMirrorTasks() && okToPutReplicaOnHost(next, task)) {
                hostStateIterator.remove();
                otherHosts.add(next);
                return new JobTaskMoveAssignment(task, fromHostId, nextId, !live, false, false);
            }
        }
        return null;
    }

    /* For each active job, ensure that the given host has a fair share of tasks from that job */
    private List<JobTaskMoveAssignment> balanceActiveJobsOnHost(HostState host, List<HostState> hosts) {
        int totalTasksToMove = config.getTasksMovedFullRebalance();
        long totalBytesToMove = config.getBytesMovedFullRebalance();
        Set<String> activeJobs = findActiveJobIDs();
        List<JobTaskMoveAssignment> rv = new ArrayList<JobTaskMoveAssignment>();
        String hostID = host.getHostUuid();
        for (String jobID : activeJobs) {
            Job job = spawn.getJob(jobID);
            if (job != null) {
                job.lock();
                try {
                    JobTaskItemByHostMap tasksByHost = new JobTaskItemByHostMap(hosts, config.getTasksMovedPerUnspecifiedHost(), config.getTasksMovedPerUnspecifiedHost());
                    for (JobTask task : job.getCopyOfTasks()) {
                        tasksByHost.addLiveAndReplicasForTask(task);
                    }
                    int maxPerHost = maxTasksPerHost(job, hosts.size());
                    int numExistingTasks = tasksByHost.get(hostID).size();
                    if ((tasksByHost.findLeastTasksOnHost() >= maxPerHost - 1 || tasksByHost.findMostTasksOnHost() <= maxPerHost)) {
                        continue;
                    }
                    boolean pushFrom = numExistingTasks > maxPerHost || numExistingTasks == maxPerHost && tasksByHost.findLeastTasksOnHost() < maxPerHost - 1;
                    if (totalTasksToMove > 0) {
                        MoveAssignmentList assignments = pushFrom ? moveTasksOffHost(tasksByHost, maxPerHost, 1, totalBytesToMove, host.getHostUuid())
                                : moveTasksOntoHost(tasksByHost, maxPerHost, 1, totalBytesToMove, host.getHostUuid());
                        rv.addAll(assignments);
                        totalTasksToMove -= assignments.size();
                        totalBytesToMove -= assignments.getBytesUsed();
                    } else {
                        break;
                    }
                } finally {
                    job.unlock();
                }
            }
        }
        return rv;
    }

    public long getTaskTrueSize(JobTask task) {
        return taskSizer.estimateTrueSize(task);
    }

    private static long getAvailDiskBytes(HostState host) {
        if (host.getMax() == null || host.getUsed() == null) {
            return 1; // Fix some tests
        }
        return host.getMax().getDisk() - host.getUsed().getDisk();
    }

    private static double getUsedDiskPercent(HostState host) {
        if (host.getMax() == null || host.getUsed() == null || host.getMax().getDisk() <= 0) {
            return 0;
        }
        return ((double) (host.getUsed().getDisk()) / host.getMax().getDisk());
    }

    /**
     * Prune a tentative list of task reassignments, removing illegal moves or moves to overburdened hosts
     *
     * @param candidateAssignments The initial list of assignments
     * @return A list of assignments with illogical moves removed
     */
    private List<JobTaskMoveAssignment> pruneTaskReassignments(List<JobTaskMoveAssignment> candidateAssignments) {
        List<JobTaskMoveAssignment> rv = new ArrayList<>();
        HashMap<String, Boolean> snapshot = new HashMap<>(recentlyBalancedHosts.asMap());
        for (JobTaskMoveAssignment assignment : candidateAssignments) {
            String newHostID = assignment.getTargetUUID();
            if (isExtremeHost(newHostID, true, true)) {
                log.warn("[spawn.balancer] decided not to move task onto " + newHostID + " because it is already heavily loaded");
                continue;
            }
            HostState newHost = spawn.getHostState(newHostID);
            JobKey jobKey = assignment.getTask().getJobKey();
            if (newHost.hasLive(jobKey) || !canReceiveNewTasks(newHost, assignment.isFromReplica())) {
                log.warn("[spawn.balancer] decided not to move task onto " + newHostID + " because it cannot receive the new task");
                continue;
            }
            if (snapshot.containsKey(newHostID)) {
                log.warn("[spawn.balancer] decided not to move task onto " + newHostID + " because it already received a different task recently");
                continue;
            }
            rv.add(assignment);
            recentlyBalancedHosts.put(newHostID, true);
        }
        return rv;
    }

    /**
     * Sorts the hosts based on their load as measured by hostScores, lightest to heaviest.
     *
     * @param hosts - the hosts to sort
     * @return the sorted list of hosts, light to heavy.
     */
    @VisibleForTesting
    protected List<HostState> sortHostsByActiveTasks(Collection<HostState> hosts) {
        List<HostState> hostList = new ArrayList<HostState>(hosts);
        removeDownHosts(hostList);
        Collections.sort(hostList, new Comparator<HostState>() {
            @Override
            public int compare(HostState hostState, HostState hostState1) {
                return Double.compare(countTotalActiveTasksOnHost(hostState), countTotalActiveTasksOnHost(hostState1));
            }
        });
        return hostList;
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

    private void removeDownHosts(List<HostState> hosts) {
        Iterator<HostState> hostIter = hosts.iterator();
        while (hostIter.hasNext()) {
            HostState host = hostIter.next();
            if (host.isDead() || !host.isUp()) {
                hostIter.remove();
            }
        }
    }

    /**
     * Internal function that creates a map of HostStates to their hostScores.
     *
     * @param hosts The hosts from spawn
     * @param jobID is specified, adds a factor that scales with the number of siblings on each host from that job.
     * @return A map taking each HostState to its score
     */
    private Map<HostState, Double> generateHostStateScoreMap(Collection<HostState> hosts, String jobID) {
        final Map<HostState, Double> hostScores = new HashMap<>(hosts.size());
        for (HostState host : hosts) {
            if (host != null && host.isUp() && !host.isDead()) {
                int siblingScore = jobID != null ? host.getTaskCount(jobID) * config.getSiblingWeight() : 0;
                double score = getHostScoreCached(host.getHostUuid()) + siblingScore;
                hostScores.put(host, score);
            }
        }
        return hostScores;
    }

    /**
     * Count the number of tasks per host for a single job, then add in a small factor for how heavily weighted each host's disk is
     *
     * @param job The job to count
     * @return A map describing how heavily a job is assigned to each of its hosts
     */
    private Map<String, Double> generateTaskCountHostScoreMap(Job job) {
        Map<String, Double> rv = new HashMap<>();
        if (job != null) {
            List<JobTask> tasks = job.getCopyOfTasks();
            for (JobTask task : tasks) {
                rv.put(task.getHostUUID(), addOrIncrement(rv.get(task.getHostUUID()), 1d));
                if (task.getReplicas() == null) {
                    continue;
                }
                for (JobTaskReplica replica : task.getReplicas()) {
                    rv.put(replica.getHostUUID(), addOrIncrement(rv.get(replica.getHostUUID()), 1d));
                }
            }
            for (HostState host : spawn.listHostStatus(job.getMinionType())) {
                if (host.isUp() && !host.isDead()) {
                    double availDisk = 1 - getUsedDiskPercent(host);
                    rv.put(host.getHostUuid(), addOrIncrement(rv.get(host.getHostUuid()), availDisk));
                }
            }
        }
        return rv;
    }

    private static double addOrIncrement(Double currentValue, Double value) {
        if (currentValue != null) {
            return currentValue + value;
        } else {
            return value;
        }
    }

    /**
     * Decide if spawn is in a good state to perform an autobalance.
     *
     * @return True if it is okay to autobalance
     */
    public boolean okayToAutobalance() {
        // Don't autobalance if it is disabled, spawn is quiesced, or the number of queued tasks is high
        if (config.getAutoBalanceLevel() == 0 || spawn.getSettings().getQuiesced() || spawn.getLastQueueSize() > spawn.listHostStatus(null).size()) {
            return false;
        }
        // Don't autobalance if there are still jobs in rebalance state
        for (Job job : spawn.listJobsConcurrentImmutable()) {
            if (JobState.REBALANCE.equals(job.getState())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is this job unbalanced enough to warrant a rebalance?
     *
     * @param job   The job to check
     * @param hosts The hosts in the cluster
     * @return True if it is appropriate to rebalance the job
     */
    private synchronized boolean shouldAutobalanceJob(Job job, List<HostState> hosts) {
        if (job == null || recentlyAutobalancedJobs.getIfPresent(job.getId()) != null || !JobState.IDLE.equals(job.getState())
            || job.getDontAutoBalanceMe() || job.getRunCount() < 1) {
            return false;
        }
        if (config.getAutoBalanceLevel() >= 2) {
            // At level 2, rebalance every job that hasn't already been rebalanced recently
            return true;
        }
        JobTaskItemByHostMap tasksByHost = generateTaskCountByHost(hosts, job.getCopyOfTasks());
        int maxPerHost = maxTasksPerHost(job, hosts.size());
        // If any host has sufficiently more or less than the expected fair share, this job is unbalanced.
        return tasksByHost.findLeastTasksOnHost() <= maxPerHost - 2 || tasksByHost.findMostTasksOnHost() >= maxPerHost + 1;
    }

    protected enum RebalanceType {HOST, JOB}

    protected enum RebalanceWeight {LIGHT, MEDIUM, HEAVY}

    /**
     * Find some task move assignments to autobalance the cluster
     *
     * @param type   Whether to balance hosts or jobs
     * @param weight Whether to balance light, medium, or heavy items of the chosen type
     * @return A list of assignments to perform the specified balancing operation
     */
    public List<JobTaskMoveAssignment> getAssignmentsForAutoBalance(RebalanceType type, RebalanceWeight weight) {
        List<HostState> hosts = spawn.getLiveHostsByReadOnlyStatus(null, false);
        switch (type) {
            case HOST:
                if (hosts.isEmpty()) {
                    return null;
                }
                List<HostState> hostsSorted = new ArrayList<>(hosts);
                Collections.sort(hostsSorted, hostStateScoreComparator);
                HostState hostToBalance = hostsSorted.get(getWeightedElementIndex(hostsSorted.size(), weight));
                return getAssignmentsToBalanceHost(hostToBalance, spawn.listHostStatus(hostToBalance.getMinionTypes()));
            case JOB:
                List<Job> autobalanceJobs = getJobsToAutobalance(hosts);
                if (autobalanceJobs == null || autobalanceJobs.isEmpty()) {
                    return null;
                }
                Job jobToBalance = autobalanceJobs.get(getWeightedElementIndex(autobalanceJobs.size(), weight));
                recentlyAutobalancedJobs.put(jobToBalance.getId(), true);
                return getAssignmentsForJobReallocation(jobToBalance, -1, spawn.listHostStatus(jobToBalance.getMinionType()));
            default:
                return null;
        }
    }

    public List<Job> getJobsToAutobalance(List<HostState> hosts) {
        List<Job> autobalanceJobs = new ArrayList<>();
        for (Job job : spawn.listJobsConcurrentImmutable()) {
            if (shouldAutobalanceJob(job, hosts)) {
                autobalanceJobs.add(job);
            }
        }
        Collections.sort(autobalanceJobs, jobAverageTaskSizeComparator);
        return autobalanceJobs;
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
                return 0;
        }
    }

    /**
     * A cached version of getHostScore that will keep host score calculation outside the main spawn thread
     *
     * @param hostId The host to measure
     * @return A non-negative number representing load.
     */
    protected double getHostScoreCached(String hostId) {
        double defaultScore = config.getDefaultHostScore();
        if (hostId == null) {
            return defaultScore;
        }
        aggregateStatisticsLock.lock();
        try {
            if (cachedHostScores == null) {
                return defaultScore;
            }
            HostScore score = cachedHostScores.get(hostId);
            if (score != null) {
                return score.getOverallScore();
            } else {
                return defaultScore;
            }
        } finally {
            aggregateStatisticsLock.unlock();
        }
    }

    /**
     * Check the live and replica hosts for a given task to see whether any of these hosts has a nearly-full disk
     *
     * @param task The task to check
     * @return True if at least one host is near full
     */
    protected boolean hasFullDiskHost(JobTask task) {
        List<HostState> hostsToCheck = new ArrayList<>();
        hostsToCheck.add(spawn.getHostState(task.getHostUUID()));
        if (task.getReplicas() != null) {
            for (JobTaskReplica replica : task.getReplicas()) {
                if (replica != null) {
                    hostsToCheck.add(spawn.getHostState(replica.getHostUUID()));
                }
            }
            for (HostState host : hostsToCheck) {
                if (host != null && isDiskFull(host)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Given a job, for each task, remove duplicate replicas, replicas pointing at the live host
     *
     * @param job      The job to modify
     * @param readOnly Whether regular or readonly replicas should be modified
     */
    protected void removeInvalidReplicas(Job job, boolean readOnly) {
        if (job != null && job.getCopyOfTasks() != null) {
            List<JobTask> tasks = job.getCopyOfTasks();
            for (JobTask task : tasks) {
                List<JobTaskReplica> newReplicas = readOnly ? new ArrayList<JobTaskReplica>(job.getReadOnlyReplicas()) : new ArrayList<JobTaskReplica>(job.getReplicas());
                if (readOnly ? task.getReadOnlyReplicas() != null : task.getReplicas() != null) {
                    List<JobTaskReplica> oldReplicas = readOnly ? new ArrayList<>(task.getReadOnlyReplicas()) : new ArrayList<>(task.getReplicas());
                    for (JobTaskReplica replica : oldReplicas) {
                        List<String> replicasSeen = new ArrayList<>();
                        String replicaHostID = replica.getHostUUID();
                        if (spawn.getHostState(replicaHostID) == null) {
                            log.warn("[spawn.balancer] removing replica for missing host " + replicaHostID);
                        } else if ((readOnly && !job.getQueryConfig().getCanQuery()) || replicaHostID.equals(task.getHostUUID()) || replicasSeen.contains(replicaHostID)) {
                            log.warn("[spawn.balancer] removing erroneous replica for " + task.getJobKey() + " on " + replicaHostID);
                        } else if (!readOnly && isReadOnlyHost(replicaHostID)) {
                            log.warn("[spawn.balancer] removing non-readonly replica for " + task.getJobKey() + " from readonly host " + replicaHostID);
                        } else if (readOnly && !isReadOnlyHost(replicaHostID)) {
                            log.warn("[spawn.balancer] removing readonly replica for " + task.getJobKey() + " from non-readonly host " + replicaHostID);
                        } else if ((!config.allowSameHostReplica()) && onSameHost(replicaHostID, task.getHostUUID())) {
                            log.warn("[spawn.balancer] removing replica on same host for " + task.getJobKey() + "live=" + task.getHostUUID() + " replica=" + replicaHostID);
                        } else {
                            replicasSeen.add(replicaHostID);
                            newReplicas.add(replica);
                        }
                    }
                }
                if (readOnly) {
                    task.setReadOnlyReplicas(newReplicas);
                } else {
                    task.setReplicas(newReplicas);
                }
            }
        }
    }

    private boolean onSameHost(String hostID1, String hostID2) {
        HostState host1 = spawn.getHostState(hostID1);
        HostState host2 = spawn.getHostState(hostID2);
        if (host1 == null || host2 == null) {
            return false;
        } else {
            return host1.getHost().equals(host2.getHost());
        }
    }

    public boolean canReceiveNewTasks(HostState host, boolean isReplica) {
        if (host == null || (!isReplica && host.isReadOnly())) {
            return false;
        }
        return host.canMirrorTasks() && getUsedDiskPercent(host) < 1 - config.getMinDiskPercentAvailToReceiveNewTasks();
    }

    /**
     * Does this host have a nearly full disk?
     *
     * @param host The host to check
     * @return True if the disk is nearly full
     */
    protected boolean isDiskFull(HostState host) {
        boolean full = host != null && getUsedDiskPercent(host) > 1 - config.getMinDiskPercentAvailToRunJobs();
        if (full) {
            log.warn("[spawn.balancer] Host " + host.getHost() + " with uuid " + host.getHostUuid() + " is nearly full, with used disk " + getUsedDiskPercent(host));
        }
        return full;
    }

    /**
     * Given a job, decide where to put additional replicas so that every task will have its full quantity of replicas.
     *
     * @param job      The job in question (not altered)
     * @param taskID   The task that needs replicas, or -1 for all tasks
     * @param readOnly Whether to assign new read-only replicas or regular replicas
     * @return Map sending nodeid => list of host IDs for which to make new replicas
     */
    protected Map<Integer, List<String>> getAssignmentsForNewReplicas(Job job, int taskID, boolean readOnly) {
        Map<Integer, List<String>> rv = new HashMap<>();
        if (job == null || (readOnly && job.getQueryConfig() != null && !job.getQueryConfig().getCanQuery())) {
            return rv;
        }
        int replicaCount = readOnly ? job.getReadOnlyReplicas() : job.getReplicas();
        Map<String, Double> scoreMap = generateTaskCountHostScoreMap(job);
        PriorityQueue<HostAndScore> scoreHeap = new PriorityQueue<>(1, hostAndScoreComparator);
        for (Entry<String, Double> entry : scoreMap.entrySet()) {
            scoreHeap.add(new HostAndScore(spawn.getHostState(entry.getKey()), entry.getValue()));
        }
        Map<String, Integer> allocationMap = new HashMap<>();
        List<JobTask> tasks = taskID > 0 ? Arrays.asList(job.getTask(taskID)) : job.getCopyOfTasks();
        for (JobTask task : tasks) {
            int numExistingReplicas = readOnly ?
                                      (task.getReadOnlyReplicas() != null ? task.getReadOnlyReplicas().size() : 0)
                                               : (task.getReplicas() != null ? task.getReplicas().size() : 0);
            List<String> hostIDsToAdd = new ArrayList<>(replicaCount);
            // Add new replicas as long as the task needs them & there are remaining hosts
            for (int i = 0; i < replicaCount - numExistingReplicas; i++) {
                for (HostAndScore hostAndScore : scoreHeap) {
                    HostState candidateHost = hostAndScore.host;
                    if (candidateHost == null || !candidateHost.canMirrorTasks()) {
                        continue;
                    }
                    int currentCount = allocationMap.containsKey(candidateHost.getHostUuid()) ? allocationMap.get(candidateHost.getHostUuid()) : 0;
                    if (readOnly && currentCount >= config.getMaxReadonlyReplicas()) {
                        continue;
                    }
                    if ((candidateHost.isReadOnly() == readOnly) && okToPutReplicaOnHost(candidateHost, task)) {
                        hostIDsToAdd.add(candidateHost.getHostUuid());
                        scoreHeap.remove(hostAndScore);
                        scoreHeap.add(new HostAndScore(candidateHost, hostAndScore.score + 1));
                        allocationMap.put(candidateHost.getHostUuid(), currentCount + 1);
                        break;
                    }
                }
            }
            if (!hostIDsToAdd.isEmpty()) {
                rv.put(task.getTaskID(), hostIDsToAdd);
            }
        }
        return rv;
    }

    public Map<Integer, List<String>> getAssignmentsForNewReplicas(Job job, boolean readOnly) {
        return getAssignmentsForNewReplicas(job, -1, readOnly);
    }

    private boolean isReadOnlyHost(String hostUUID) {
        HostState hostState = spawn.getHostState(hostUUID);
        return hostState != null && hostState.isReadOnly();
    }

    /**
     * Is it acceptable to put a replica of this task on this host? (task can't have a live and replica version on host)
     *
     * @param hostCandidate The host that is being considered to house the new replica
     * @param task          The task that might be replicated to that host
     * @return True if it is okay to put a replica on the host
     */
    private boolean okToPutReplicaOnHost(HostState hostCandidate, JobTask task) {
        Job job;
        String hostId;
        if (hostCandidate == null || ((hostId = hostCandidate.getHostUuid()) == null) || !canReceiveNewTasks(hostCandidate, true) || ((job = spawn.getJob(task.getJobKey())) == null) || !hostCandidate.getMinionTypes().contains(job.getMinionType())) {
            return false;
        }
        if (spawn.getHostFailWorker().getFailureState(hostId) != HostFailWorker.FailState.ALIVE) {
            return false;
        }
        HostState taskHost = spawn.getHostState(task.getHostUUID());
        /* Protect against npe in case the existing host has disappeared somehow */
        String existingHost = taskHost != null ? taskHost.getHost() : null;
        /* in non-local-stack, prevent replicates to same host (multi-minion-per-host-setup) */
        if (!config.allowSameHostReplica() && hostCandidate.getHost().equals(existingHost)) {
            return false;
        }
        /* don't let the same minion have duplicate tasks */
        if (task.getHostUUID().equals(hostCandidate.getHostUuid()) || task.hasReplicaOnHost(hostCandidate.getHostUuid())) {
            return false;
        }
        /* try not to put a task on a host if it would almost fill the host */
        if (taskSizer.estimateTrueSize(task) > config.getHostDiskFactor() * getAvailDiskBytes(hostCandidate)) {
            return false;
        }
        return true;
    }

    private static class HostAndScore {

        private final HostState host;
        private final double score;

        private HostAndScore(HostState host, double score) {
            this.host = host;
            this.score = score;
        }
    }

    /**
     * Update aggregate cluster statistics periodically
     */
    private class AggregateStatUpdaterTask extends TimerTask {

        @Override
        public void run() {
            if (JitterClock.globalTime() - lastAggregateStatUpdateTime > AGGREGATE_STAT_UPDATE_INTERVAL) {
                for (String minionType : spawn.listMinionTypes()) {
                    /* Score the minions within a given type only against minions at the same type.
                    Since disk space can vary wildly between different minion types (e.g. ssd vs. non-ssd),
                    it doesn't make sense to compare different minion types. */
                    updateAggregateStatistics(spawn.listHostStatus(minionType));
                }
            }
        }
    }

    private Set<String> findActiveJobIDs() {
        aggregateStatisticsLock.lock();
        try {
            activeJobIDs = new HashSet<>();
            Collection<Job> jobs = spawn.listJobsConcurrentImmutable();
            if (jobs != null && !jobs.isEmpty()) {
                for (Job job : jobs) {
                    if (isWellFormedAndActiveJob(job)) {
                        activeJobIDs.add(job.getId());
                    }
                }
            }
            return new HashSet<>(activeJobIDs);
        } finally {
            aggregateStatisticsLock.unlock();
        }

    }

    private int countTotalActiveTasksOnHost(HostState host) {
        int count = 0;
        if (host != null) {
            host.generateJobTaskCountMap();
            aggregateStatisticsLock.lock();
            try {
                if (activeJobIDs == null) {
                    activeJobIDs = findActiveJobIDs();
                }
                for (String jobID : activeJobIDs) {
                    count += host.getTaskCount(jobID);
                }
            } finally {
                aggregateStatisticsLock.unlock();
            }
        }

        return count;
    }

    /**
     * Update SpawnBalancer's cluster-wide metrics, including host scores and active jobs.
     *
     * @param hosts A list of HostStates
     */
    protected void updateAggregateStatistics(List<HostState> hosts) {
        aggregateStatisticsLock.lock();
        try {
            lastAggregateStatUpdateTime = JitterClock.globalTime();
            findActiveJobIDs();
            double maxMeanActive = -1;
            double maxDiskPercentUsed = -1;
            for (HostState host : hosts) {
                maxMeanActive = Math.max(maxMeanActive, host.getMeanActiveTasks());
                maxDiskPercentUsed = Math.max(maxDiskPercentUsed, getUsedDiskPercent(host));
            }
            for (HostState host : hosts) {
                cachedHostScores.put(host.getHostUuid(), calculateHostScore(host, maxMeanActive, maxDiskPercentUsed));
            }
        } finally {
            aggregateStatisticsLock.unlock();
        }
    }

    public void requestJobSizeUpdate(String jobId, int taskId) {
        taskSizer.requestJobSizeFetch(jobId, taskId);
    }

    /**
     * Is this host's load significantly different from the rest of the cluster?
     *
     * @param hostID    The host to check
     * @param diskSpace Whether to consider load based on disk space (as opposed to number of tasks)
     * @param high      Whether to look for heavy load as opposed to light load
     * @return True if the host has the specified level of load
     */
    protected boolean isExtremeHost(String hostID, boolean diskSpace, boolean high) {
        aggregateStatisticsLock.lock();
        try {
            if (cachedHostScores == null || !cachedHostScores.containsKey(hostID) || cachedHostScores.isEmpty()) {
                return false;
            }
            double clusterAverage = 0;
            for (HostScore score : cachedHostScores.values()) {
                clusterAverage += score.getScoreValue(diskSpace);
            }
            clusterAverage /= cachedHostScores.size(); // Nonzero as we check if cachedHostScores.isEmpty first
            double hostValue = cachedHostScores.get(hostID).getScoreValue(diskSpace);
            return ((high && hostValue > clusterAverage * config.getExtremeHostRatio()) ||
                    (!high && hostValue < clusterAverage / config.getExtremeHostRatio()));
        } finally {
            aggregateStatisticsLock.unlock();
        }

    }

    private HostScore calculateHostScore(HostState host, double clusterMaxMeanActive, double clusterMaxDiskUsed) {
        double score = 0;
        double meanActive = host.getMeanActiveTasks();
        double usedDiskPercent = getUsedDiskPercent(host);
        // If either metric is zero across the whole cluster, treat every host as having full load in that aspect
        if (clusterMaxMeanActive <= 0) {
            meanActive = 1;
            clusterMaxMeanActive = 1;
        }
        if (clusterMaxDiskUsed <= 0) {
            usedDiskPercent = 1;
            clusterMaxDiskUsed = 1;
        }
        int activeTaskWeight = config.getActiveTaskWeight();
        int diskUsedWeight = config.getDiskUsedWeight();
        // Assemble the score as a combination of the mean active tasks and the disk used
        score += activeTaskWeight * Math.pow(meanActive / clusterMaxMeanActive, 2.5);
        score += diskUsedWeight * Math.pow(usedDiskPercent / clusterMaxDiskUsed, 2.5);
        // If host is very full, make sure to give the host a big score
        score = Math.max(score, (activeTaskWeight + diskUsedWeight) * usedDiskPercent);
        return new HostScore(meanActive, usedDiskPercent, score);
    }

    private class HostScore {

        private final double meanActiveTasks;
        private final double usedDiskPercent;
        private final double overallScore;

        private HostScore(double meanActiveTasks, double usedDiskPercent, double overallScore) {
            this.meanActiveTasks = meanActiveTasks;
            this.usedDiskPercent = usedDiskPercent;
            this.overallScore = overallScore;
        }

        public double getMeanActiveTasks() {
            return meanActiveTasks;
        }

        public double getUsedDiskPercent() {
            return usedDiskPercent;
        }

        public double getOverallScore() {
            return overallScore;
        }

        public double getScoreValue(boolean diskSpace) {
            return diskSpace ? usedDiskPercent : meanActiveTasks;
        }

    }

    private class MoveAssignmentList extends ArrayList<JobTaskMoveAssignment> {

        private long bytesUsed = 0;

        @Override
        public boolean add(JobTaskMoveAssignment assignment) {
            bytesUsed += taskSizer.estimateTrueSize(assignment.getTask());
            return super.add(assignment);
        }

        public long getBytesUsed() {
            return bytesUsed;
        }
    }

    public void setAllowSameHostReplica(boolean allowSameHostReplica) {
        config.setAllowSameHostReplica(allowSameHostReplica);
    }

    public SpawnBalancerConfig getConfig() {
        return config;
    }

    public void setConfig(SpawnBalancerConfig config) {
        this.config = config;
    }

    /**
     * Start a thread that will perform autobalancing in the background if appropriate to do so
     */
    public synchronized void startAutobalanceTask() {
        if (autobalanceStarted.compareAndSet(false, true)) {
            new Timer(true).scheduleAtFixedRate(new AutobalanceTimerTask(), config.getAutobalanceCheckInterval(), config.getAutobalanceCheckInterval());
        }
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
        List<JobTask> tasks = findAllTasksAssignedToHost(failedHost);
        ArrayList<JobTask> sortedTasks = new ArrayList<>(tasks);
        Collections.sort(sortedTasks, new Comparator<JobTask>() {
            @Override
            public int compare(JobTask o1, JobTask o2) {
                return Long.compare(taskSizer.estimateTrueSize(o1), taskSizer.estimateTrueSize(o2));
            }
        });
        hosts = sortHostsByDiskSpace(hosts);
        HashSet<String> modifiedJobIds = new HashSet<>();
        for (JobTask task : sortedTasks) {
            modifiedJobIds.add(task.getJobUUID());
            try {
                attemptFixTaskForFailedHost(task, hosts, failedHost);
            } catch (Exception ex) {
                log.warn("Warning: failed to recover task " + task.getJobKey() + ": " + ex, ex);
                }
        }
        for (String jobId : modifiedJobIds) {
            try {
                spawn.updateJob(spawn.getJob(jobId));
            } catch (Exception e) {
                log.warn("Warning: failed to update job " + jobId + ": " + e, e);
                }
        }
    }

    private List<JobTask> findAllTasksAssignedToHost(String failedHostUUID) {
        List<JobTask> rv = new ArrayList<>();
        for (Job job : spawn.listJobsConcurrentImmutable()) {
            if (job != null) {
                for (JobTask task : job.getCopyOfTasks()) {
                    if (task != null && (task.getHostUUID().equals(failedHostUUID) || task.hasReplicaOnHost(failedHostUUID))) {
                        rv.add(task);
                    }
                }
            }
        }
        return rv;
    }

    /**
     * For a particular task with a copy on a failed host, attempt to find a suitable replica; mark it degraded otherwise
     *
     * @param task           The task to modify
     * @param hosts          A list of available hosts
     * @param failedHostUuid The host being failed
     */
    private void attemptFixTaskForFailedHost(JobTask task, List<HostState> hosts, String failedHostUuid) {
        Iterator<HostState> hostIterator = hosts.iterator();
        Job job;
        if (task == null || task.getJobUUID() == null || (job = spawn.getJob(task.getJobUUID())) == null) {
            log.warn("Skipping nonexistent job for task " + task + " during host fail.");
            return;
        }
        if (task.getReplicas() == null || task.getReplicas().isEmpty()) {
            log.warn("Found no replica for task " + task.getJobKey());
            job.setState(JobState.DEGRADED, true);
            return;
        }
        if (!task.getHostUUID().equals(failedHostUuid) && !task.hasReplicaOnHost(failedHostUuid)) {
            // This task was not actually assigned to the failed host. Nothing to do.
            return;
        }
        while (hostIterator.hasNext()) {
            HostState host = hostIterator.next();
            if (host.getHostUuid().equals(failedHostUuid)) {
                continue;
            }
            if (host.canMirrorTasks() && okToPutReplicaOnHost(host, task)) {
                // Host found! Move this host to the end of the host list so we don't immediately pick it again
                hostIterator.remove();
                hosts.add(host);
                executeHostFailureRecovery(task, failedHostUuid, host);
                return;
            }
        }
        log.warn("Failed to find a host that could hold " + task.getJobKey() + " after host failure");
        job.setState(JobState.DEGRADED, true);
    }

    /**
     * Modify the live/replica copies of a task to handle a failed host
     *
     * @param task           The task to be modified
     * @param failedHostUuid The host being failed
     * @param newReplicaHost A host that will receive a new copy of the data
     */
    private void executeHostFailureRecovery(JobTask task, String failedHostUuid, HostState newReplicaHost) {
        boolean liveOnFailedHost = task.getHostUUID().equals(failedHostUuid);
        String newReplicaUuid = newReplicaHost.getHostUuid();
        if (liveOnFailedHost) {
            // Send a kill message if the task is running on the failed host
            spawn.sendControlMessage(new CommandTaskStop(failedHostUuid, task.getJobUUID(), task.getTaskID(), 0, true, false));
            // Find a replica, promote it, and tell it to replicate to the new replica on completion
            String chosenReplica = task.getReplicas().get(0).getHostUUID();
            task.replaceReplica(chosenReplica, newReplicaUuid);
            task.setHostUUID(chosenReplica);
            spawn.replicateTask(task, Arrays.asList(newReplicaUuid));
        } else {
            // Replace the replica on the failed host with one on a new host
            task.replaceReplica(failedHostUuid, newReplicaUuid);
            spawn.replicateTask(task, Arrays.asList(newReplicaUuid));
        }
    }

    /**
     * This class performs job/host rebalancing at specified intervals
     */
    private class AutobalanceTimerTask extends TimerTask {

        private long lastJobAutobalanceTime = 0L;
        private long lastHostAutobalanceTime = 0L;
        private int numHostRebalances = 0;

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            // Don't autobalance if spawn is quiesced, busy, etc.
            if (okayToAutobalance()) {
                if (now - lastHostAutobalanceTime > config.getHostAutobalanceIntervalMillis()) {
                    numHostRebalances++;
                    log.warn("Performing host autobalance.");
                    RebalanceWeight weight = numHostRebalances % 2 == 0 ? RebalanceWeight.HEAVY : RebalanceWeight.LIGHT;
                    spawn.executeReallocationAssignments(getAssignmentsForAutoBalance(RebalanceType.HOST, weight));
                    lastHostAutobalanceTime = now;
                } else if (now - lastJobAutobalanceTime > config.getJobAutobalanceIntervalMillis()) {
                    log.warn("Performing job autobalance.");
                    spawn.executeReallocationAssignments(getAssignmentsForAutoBalance(RebalanceType.JOB, RebalanceWeight.HEAVY));
                    lastJobAutobalanceTime = now;
                }
            }
        }
    }
}

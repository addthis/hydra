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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;
import com.addthis.hydra.minion.Zone;

import com.google.common.collect.ImmutableSet;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class HostFailWorker {

    private static final Logger log = LoggerFactory.getLogger(HostFailWorker.class);
    private final AtomicBoolean newAdditions = new AtomicBoolean(false); // True if a host has been recently added to the queue
    private final AtomicBoolean obeyTaskSlots = new AtomicBoolean(true); // Whether spawn should honor the max task slots when moving tasks to fail hosts
    private final HostFailState hostFailState;
    private final Spawn spawn;
    private final HostManager hostManager;

    // Perform host-failure related operations at a given interval
    private static final long hostFailDelayMillis = Parameter.longValue("host.fail.delay", 15_000);
    // Quiet period between when host is failed in UI and when Spawn begins failure-related operations
    private static final long hostFailQuietPeriod = Parameter.longValue("host.fail.quiet.period", 20_000);

    // Don't rebalance additional tasks if spawn is already rebalancing at least this many.
    private static final int maxMovingTasks = Parameter.intValue("host.fail.maxMovingTasks", 6);
    // Use a smaller max when a disk is being failed, to avoid a 'thundering herds' scenario
    private static final int maxMovingTasksDiskFull = Parameter.intValue("host.fail.maxMovingTasksDiskFull", 2);

    private static final Counter failHostCount = Metrics.newCounter(Spawn.class, "failHostCount");

    // Various keys used to make JSON objects to send to the UI
    private static final String infoHostsKey = "uuids";
    private static final String infoDeadFsKey = "deadFs";
    private static final String infoWarningKey = "warning";
    private static final String infoPrefailCapacityKey = "prefail";
    private static final String infoPostfailCapacityKey = "postfail";
    private static final String infoFatalWarningKey = "fatal";
    private final ScheduledExecutorService executorService;

    public HostFailWorker(Spawn spawn, HostManager hostManager, ScheduledExecutorService executorService) {
        this.spawn = spawn;
        this.hostManager = hostManager;
        this.executorService = executorService;
        hostFailState = new HostFailState(spawn);
        hostFailState.loadState();
    }

    /** Initializes scheduled execution of fail host task **/
    public void initFailHostTaskSchedule() {
        if (executorService != null) {
            if (hostFailState.nextHostToFail() != null) {
                queueFailNextHost();
            }
            executorService.scheduleWithFixedDelay(new FailHostTask(true), hostFailDelayMillis, hostFailDelayMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Mark a series of hosts for failure
     *
     * @param hostIds        A comma-separated list of host uuids
     * @param state      The state of the hosts being failed
     */
    public void markHostsToFail(String hostIds, FailState state) {
        if (hostIds != null) {
            for (String host : hostIds.split(",")) {
                hostFailState.putHost(host, state);
                spawn.sendHostUpdateEvent(spawn.hostManager.getHostState(host));
            }
            queueFailNextHost();
        }
    }

    /**
     * @return The set of all minion ids across all failure queues.
     */
    public Set<String> queuedHosts() {
        return hostFailState.queuedHosts();
    }

    /**
     * Retrieve an enum describing whether/how a host has been failed (for programmatic purposes)
     *
     * @param hostId A host uuid to check
     * @return A FailState object describing whether the host has been failed
     */
    public FailState getFailureState(String hostId) {
        return hostFailState.getState(hostId);
    }

    public boolean shouldKickTasks(String hostId) {
        FailState failState = getFailureState(hostId);
        // A Failing_Fs_Okay host is nominally fine for the time being. It should be allowed to run tasks.
        return failState == FailState.ALIVE || failState == FailState.FAILING_FS_OKAY;
    }

    /**
     * Retrieve a human-readable string describing whether/how a host has been failed
     *
     * @param hostId A host uuid to check
     * @param up     Whether the host is up
     * @return A String describing the host's failure state (mainly for the UI)
     */
    public String getFailureStateString(String hostId, boolean up) {
        FailState failState = getFailureState(hostId);
        switch (failState) {
            case ALIVE:
                return up ? "up" : "down";
            case FAILING_FS_DEAD:
                return "queued to fail (fs dead)";
            case FAILING_FS_OKAY:
                return "queued to fail (fs okay)";
            case DISK_FULL:
                return "disk near full; moving tasks off";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Cancel the failure of one or more hosts
     *
     * @param hostIds A comma-separated list of host uuids
     */
    public void removeHostsForFailure(String hostIds) {
        if (hostIds != null) {
            for (String host : hostIds.split(",")) {
                hostFailState.removeHost(host);
                spawn.sendHostUpdateEvent(spawn.hostManager.getHostState(host));
            }
        }
    }

    /**
     * Decide whether a given host can be failed based on whether other minions in the cluster are up
     *
     * @param failedHostUuid The host to be failed
     * @return True only if there are no down hosts that would need to be up in order to correctly fail the host
     */
    protected boolean checkHostStatesForFailure(String failedHostUuid) {
        Collection<HostState> hostStates = spawn.hostManager.listHostStatus(null);
        for (HostState hostState : hostStates) {
            if (!failedHostUuid.equals(hostState.getHostUuid()) && shouldBlockHostFailure(ImmutableSet.of(failedHostUuid), hostState)) {
                log.warn("Unable to fail host: " + failedHostUuid +
                         " because one of the minions (" + hostState.getHostUuid() +
                         ") on " + hostState.getHost() +
                         " is currently down.  Retry when all minions are available");
                return false;
            }
        }
        return true;
    }

    /**
     * Fail a host. For any tasks with replicas on that host, move these replicas elsewhere. For any tasks with live copies on the host,
     * promote a replica, then make a new replica somewhere else.
     */
    private void markHostDead(String failedHostUuid) {
        if (failedHostUuid == null || !checkHostStatesForFailure(failedHostUuid)) {
            return;
        }
        spawn.markHostStateDead(failedHostUuid);
        hostFailState.removeHost(failedHostUuid);
        failHostCount.inc();
    }

    /**
     * Before failing host(s), check if a different host needs to be up to perform the failure operation.
     *
     * @param failedHostUUIDs The hosts being failed
     * @param hostState       The host to check
     * @return True if the host is down
     */
    private boolean shouldBlockHostFailure(Set<String> failedHostUUIDs, HostState hostState) {
        if (hostState == null || hostState.isDead() || hostState.isUp()) {
            return false;
        }
        for (JobKey jobKey : hostState.allJobKeys()) // never null due to implementation
        {
            JobTask task = spawn.getTask(jobKey);
            if (task != null && (failedHostUUIDs.contains(task.getHostUUID()) || task.hasReplicaOnHosts(failedHostUUIDs))) {
                // There is a task on a to-be-failed host that has a copy on a host that is down. We cannot fail for now.
                return true;
            }
        }
        return false;
    }

    /**
     * After receiving a host failure request, queue an event to fail that host after a quiet period
     */
    private void queueFailNextHost() {
        if (newAdditions.compareAndSet(false, true)) {
            executorService.schedule(new FailHostTask(false), hostFailQuietPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Find the next host on the fail queue, considering filesystem-dead hosts first. Perform the correct actions and remove from the queue if appropriate.
     */
    private void failNextHost() {
        Pair<String, FailState> hostToFail = hostFailState.nextHostToFail();
        if (hostToFail != null) {
            String failedHostUuid = hostToFail.getLeft();
            FailState failState = hostToFail.getRight();
            if (failState == FailState.FAILING_FS_DEAD) {
                // File system is dead. Relocate all tasks ASAP.
                markHostDead(failedHostUuid);
                Zone zone = spawn.hostManager.getHostState(failedHostUuid).getZone();
                spawn.getSpawnBalancer().fixTasksForFailedHost(spawn.hostManager.listHostStatusForHostFail(null, zone), failedHostUuid);
            } else {
                HostState host = spawn.hostManager.getHostState(failedHostUuid);
                if (host == null) {
                    // Host is gone or has no more tasks. Simply mark it as failed.
                    markHostDead(failedHostUuid);
                    return;
                }
                boolean diskFull = (failState == FailState.DISK_FULL);
                if (!diskFull && spawn.getSystemManager().isQuiesced()) {
                    // If filesystem is okay, don't do any moves while spawn is quiesced.s
                    return;
                }
                int taskMovingMax = diskFull ? maxMovingTasksDiskFull : maxMovingTasks;
                int tasksRebalancing = countRebalancingTasks();
                int tasksToMove = taskMovingMax - tasksRebalancing;
                if (tasksToMove <= 0) {
                    // Spawn is already moving enough tasks; hold off until later
                    return;
                }
                List<JobTaskMoveAssignment> assignments = spawn.getSpawnBalancer().pushTasksOffHostForFilesystemOkayFailure(host, tasksToMove);
                // no re-assignments available for this host, move it to the end of the fs-ok queue
                if(assignments.isEmpty() && tasksRebalancing == 0 && failState == FailState.FAILING_FS_OKAY) {
                    hostFailState.removeHost(failedHostUuid);
                    hostFailState.putHost(failedHostUuid, FailState.FAILING_FS_OKAY);
                }
                // Use available task slots to push tasks off the host in question. Not all of these assignments will necessarily be moved.
                spawn.executeReallocationAssignments(assignments, !diskFull && obeyTaskSlots.get());
                if (failState == FailState.FAILING_FS_OKAY && assignments.isEmpty() && host.countTotalLive() == 0) {
                    // Found no tasks on the failed host, so fail it for real.
                    markHostDead(failedHostUuid);
                    Zone zone = spawn.hostManager.getHostState(failedHostUuid).getZone();
                    spawn.getSpawnBalancer().fixTasksForFailedHost(
                            spawn.hostManager.listHostStatusForHostFail(host.getMinionTypes(), zone), failedHostUuid);
                }
            }
        }
    }

    public void setObeyTaskSlots(boolean obey) {
        obeyTaskSlots.set(obey);
    }

    /**
     * Retrieve information about the implications of failing a host, to inform/warn a user in the UI
     *
     * @param hostsToFail The hosts that will be failed
     * @return A JSONObject with various data about the implications of the failure
     */
    public JSONObject getInfoForHostFailure(String hostsToFail, boolean deadFilesystem) throws JSONException {
        if (hostsToFail == null) {
            return new JSONObject();
        }
        HashSet<String> ids = new HashSet<>(Arrays.asList(hostsToFail.split(",")));
        long totalClusterAvail = 0, totalClusterUsed = 0, hostAvail = 0;
        List<String> hostsDown = new ArrayList<>();
        for (HostState host : spawn.hostManager.listHostStatus(null)) {
            // Sum up disk availability across the entire cluster and across the specified hosts
            if (host.getMax() != null && host.getUsed() != null) {
                if (getFailureState(host.getHostUuid()) == FailState.ALIVE) {
                    totalClusterAvail += host.getMax().getDisk();
                    totalClusterUsed += host.getUsed().getDisk();
                }
                if (ids.contains(host.getHostUuid())) {
                    hostAvail += host.getMax().getDisk();
                }
            }
            if (!ids.contains(host.getHostUuid()) && shouldBlockHostFailure(ids, host)) {
                hostsDown.add(host.getHostUuid() + " on " + host.getHost());
            }
        }
        // Guard against division by zero in the case of unexpected values
        totalClusterAvail = Math.max(1, totalClusterAvail);
        hostAvail = Math.min(totalClusterAvail - 1, hostAvail);
        return constructInfoMessage(hostsToFail, deadFilesystem, (double) (totalClusterUsed) / totalClusterAvail, (double) (totalClusterUsed) / (totalClusterAvail - hostAvail), hostsDown);
    }

    /**
     * Create the info message about host message using some raw values
     *
     * @param prefailCapacity  The capacity the cluster had before the failure
     * @param postfailCapacity The capacity the cluster would have after the failure
     * @param hostsDown        Any hosts that are down that might temporarily prevent failure
     * @return A JSONObject encapsulating the above information.
     * @throws JSONException
     */
    private JSONObject constructInfoMessage(String hostsToFail, boolean deadFilesystem, double prefailCapacity,
                                            double postfailCapacity, List<String> hostsDown) throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(infoHostsKey, hostsToFail);
        obj.put(infoDeadFsKey, deadFilesystem);
        obj.put(infoPrefailCapacityKey, prefailCapacity);
        obj.put(infoPostfailCapacityKey, postfailCapacity);
        if (Double.isNaN(postfailCapacity)) {
            obj.put(infoFatalWarningKey, "Cannot fail all hosts from a cluster");
        } else if (postfailCapacity >= 1) {
            obj.put(infoFatalWarningKey, "Insufficient cluster disk capacity");
        }
        if (!hostsDown.isEmpty()) {
            obj.put(infoWarningKey, "Some hosts are down. Host failure could be delayed until they return: " + hostsDown);
        }
        return obj;
    }


    /**
     * A simple wrapper around failNextHost that is run by the failExecutor.
     */
    private class FailHostTask implements Runnable {

        private final boolean skipIfNewAdditions;

        public FailHostTask(boolean skipIfNewAdditions) {
            this.skipIfNewAdditions = skipIfNewAdditions;
        }

        @Override
        public void run() {
            updateFullMinions();
            if (skipIfNewAdditions && newAdditions.get()) {
                return;
            }
            try {
                failNextHost();
            } catch (Exception e) {
                log.warn("Exception while failing host: {}", e.getMessage(), e);
            } finally {
                newAdditions.set(false);
            }
        }
    }

    public void updateFullMinions() {
        for (HostState hostState : spawn.hostManager.listHostStatus(null)) {
            if (hostState == null || hostState.isDead() || !hostState.isUp()) {
                continue;
            }
            String hostId = hostState.getHostUuid();
            if (spawn.getSpawnBalancer().isDiskFull(hostState)) {
                markHostsToFail(hostId, HostFailWorker.FailState.DISK_FULL);
            } else if (getFailureState(hostId) == HostFailWorker.FailState.DISK_FULL) {
                // Host was previously full, but isn't anymore. Take it off the disk_full list.
                hostFailState.removeHost(hostId);
            }
        }
    }

    private int countRebalancingTasks() {
        int count = 0;
        for (Job job : spawn.listJobs()) {
            if (job.getState() == JobState.REBALANCE) {
                for (JobTask task : job.getCopyOfTasks()) {
                    if (task.getState() == JobTaskState.REBALANCE) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    /**
     * This enum tracks HostFailWorker's ideas of Host State. Options are:
     * - ALIVE: host is normal
     * - FAILING_FS_DEAD: User has requested that the host be failed immediately. There is a quiet period to allow the
     * queue logic to exit gracefully and to allow user to cancel if there was a mistake.
     * - FAILING_FS_OKAY: User has requested that the host be failed eventually, after safely migrating each task off.
     * - DISK_FULL: HostFailWorker detects hosts that are nearly full on disk, and moves tasks off automatically. Once
     * they return to safer levels, they will go back to ALIVE status.
     */
    public enum FailState {ALIVE, FAILING_FS_DEAD, FAILING_FS_OKAY, DISK_FULL}

}

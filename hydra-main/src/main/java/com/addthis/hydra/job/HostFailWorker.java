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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.chores.JobTaskMoveAssignment;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class HostFailWorker {

    private static final Logger log = LoggerFactory.getLogger(HostFailWorker.class);
    private final HostFailState hostFailState;
    private AtomicBoolean newAdditions = new AtomicBoolean(false); // True if a host has been recently added to the queue
    private final Spawn spawn;

    // Perform host-failure related operations at a given interval
    private final long hostFailDelayMillis = Parameter.longValue("host.fail.delay", 15_000);
    // Quiet period between when host is failed in UI and when Spawn begins failure-related operations
    private final long hostFailQuietPeriod = Parameter.longValue("host.fail.quiet.period", 20_000);

    private final Timer failTimer = new Timer(true);

    private static final String dataStoragePath = "/spawn/hostfailworker";
    private static final Counter failHostCount = Metrics.newCounter(Spawn.class, "failHostCount");

    // Various keys used to make JSON objects to send to the UI
    private static final String infoHostsKey = "uuids";
    private static final String infoDeadFsKey = "deadFs";
    private static final String infoWarningKey = "warning";
    private static final String infoPrefailCapacityKey = "prefail";
    private static final String infoPostfailCapacityKey = "postfail";
    private static final String infoFatalWarningKey = "fatal";

    public HostFailWorker(Spawn spawn) {
        hostFailState = new HostFailState();
        this.spawn = spawn;
        boolean loaded = hostFailState.loadState();
        if (loaded) {
            queueFailNextHost();
        }
        failTimer.scheduleAtFixedRate(new FailHostTask(true), hostFailDelayMillis, hostFailDelayMillis);
    }

    public void stop() {
        failTimer.cancel();
    }


    /**
     * Mark a series of hosts for failure
     *
     * @param hostIds        A comma-separated list of host uuids
     * @param fileSystemDead Whether the file systems of these hosts should be treated as unreachable
     */
    public void markHostsToFail(String hostIds, boolean fileSystemDead) {
        if (hostIds != null) {
            for (String host : hostIds.split(",")) {
                hostFailState.putHost(host, fileSystemDead);
                spawn.sendHostUpdateEvent(spawn.getHostState(host));
            }
            queueFailNextHost();
        }
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
                spawn.sendHostUpdateEvent(spawn.getHostState(host));
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
        Collection<HostState> hostStates = spawn.listHostStatus(null);
        for (HostState hostState : hostStates) {
            if (!failedHostUuid.equals(hostState.getHostUuid()) && shouldBlockHostFailure(ImmutableSet.of(failedHostUuid), hostState)) {
                log.warn("Unable to fail host: " + failedHostUuid + " because one of the minions (" + hostState.getHostUuid() + ") on " + hostState.getHost() + " is currently down.  Retry when all minions are available");
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
            failTimer.schedule(new FailHostTask(false), hostFailQuietPeriod);
        }
    }

    /**
     * Find the next host on the fail queue, considering filesystem-dead hosts first. Perform the correct actions and remove from the queue if appropriate.
     */
    private void failNextHost() {
        Pair<String, Boolean> hostToFail = hostFailState.nextHostToFail();
        if (hostToFail != null) {
            String failedHostUuid = hostToFail.getLeft();
            boolean fileSystemDead = hostToFail.getRight();
            if (!fileSystemDead && spawn.getSettings().getQuiesced()) {
                // If filesystem is okay, don't do any moves while spawn is quiesced.
                return;
            }
            HostState host = spawn.getHostState(failedHostUuid);
            if (host == null) {
                // Host is gone or has no more tasks. Simply mark it as failed.
                markHostDead(failedHostUuid);
                return;
            }
            if (fileSystemDead) {
                // File system is dead. Relocate all tasks ASAP.
                markHostDead(failedHostUuid);
                spawn.getSpawnBalancer().fixTasksForFailedHost(spawn.listHostStatus(host.getMinionTypes()), failedHostUuid);
            } else if (host.getAvailableTaskSlots() > 0) {
                // File system is okay. Push some tasks off the host if it has some available capacity.
                List<JobTaskMoveAssignment> assignments = spawn.getSpawnBalancer().pushTasksOffDiskForFilesystemOkayFailure(host, host.getAvailableTaskSlots());
                spawn.executeReallocationAssignments(assignments);
                if (assignments.isEmpty() && host.countTotalLive() == 0) {
                    // Found no tasks on the failed host, so fail it for real.
                    markHostDead(failedHostUuid);
                    spawn.getSpawnBalancer().fixTasksForFailedHost(spawn.listHostStatus(host.getMinionTypes()), failedHostUuid);
                }
            }
        }
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
        for (HostState host : spawn.listHostStatus(null)) {
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
    private JSONObject constructInfoMessage(String hostsToFail, boolean deadFilesystem, double prefailCapacity, double postfailCapacity, List<String> hostsDown) throws JSONException {
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
    private class FailHostTask extends TimerTask {

        private final boolean skipIfNewAdditions;

        public FailHostTask(boolean skipIfNewAdditions) {
            this.skipIfNewAdditions = skipIfNewAdditions;
        }

        @Override
        public void run() {
            if (skipIfNewAdditions && newAdditions.get()) {
                return;
            }
            try {
                failNextHost();
            } catch (Exception e) {
                log.warn("Exception while failing host: " + e, e);
                } finally {
                newAdditions.set(false);
            }
        }
    }

    /**
     * A class storing the internal state of the host failure queue. All changes are immediately saved to the SpawnDataStore
     */
    private class HostFailState {

        private static final String filesystemDeadKey = "deadFs";
        private static final String filesystemOkayKey = "okayFs";
        private final Map<Boolean, Set<String>> hostsToFailByType = new HashMap<>();
        private final Set<String> failFsDead;
        private final Set<String> failFsOkay;

        public HostFailState() {
            synchronized (hostsToFailByType) {
                failFsDead = new LinkedHashSet<>();
                hostsToFailByType.put(false, failFsDead);
                failFsOkay = new LinkedHashSet<>();
                hostsToFailByType.put(true, failFsOkay);
            }
        }

        /**
         * Add a new failed host
         *
         * @param hostId         The host id to add
         * @param deadFileSystem Whether the host's filesystem should be treated as dead
         */
        public void putHost(String hostId, boolean deadFileSystem) {
            synchronized (hostsToFailByType) {
                if (deadFileSystem && failFsOkay.contains(hostId)) {
                    // Change a host from an fs-okay failure to an fs-dead failure
                    failFsOkay.remove(hostId);
                    failFsDead.add(hostId);
                } else if (!deadFileSystem && failFsDead.contains(hostId)) {
                    log.warn("Ignoring eventual fail host on " + hostId + " because it is already marked as dead");
                } else {
                    hostsToFailByType.get(deadFileSystem).add(hostId);
                }
                saveState();
            }
        }

        /**
         * Retrieve the next host to fail
         *
         * @return The uuid of the next host to fail, and whether the file system is dead. If the queue is empty, return null.
         */
        public Pair<String, Boolean> nextHostToFail() {
            synchronized (hostsToFailByType) {
                if (!failFsOkay.isEmpty()) {
                    return Pair.of(failFsOkay.iterator().next(), true);
                } else if (!failFsDead.isEmpty()) {
                    return Pair.of(failFsDead.iterator().next(), false);
                }
                return null;
            }
        }

        /**
         * Cancel the failure for a host
         *
         * @param hostId The uuid to cancel
         */
        public void removeHost(String hostId) {
            synchronized (hostsToFailByType) {
                for (Set<String> hosts : hostsToFailByType.values()) {
                    hosts.remove(hostId);
                }
                saveState();
            }
        }

        /**
         * Load the stored state from the SpawnDataStore
         *
         * @return True if at least one host was loaded
         */
        public boolean loadState() {
            SpawnDataStore spawnDataStore = spawn.getSpawnDataStore();
            if (spawnDataStore == null) {
                return false;
            }
            String raw = spawnDataStore.get(dataStoragePath);
            if (raw == null) {
                return false;
            }
            synchronized (hostsToFailByType) {
                try {
                    JSONObject decoded = new JSONObject(spawn.getSpawnDataStore().get(dataStoragePath));
                    loadHostsFromJSONArray(false, decoded.getJSONArray(filesystemOkayKey));
                    loadHostsFromJSONArray(true, decoded.getJSONArray(filesystemDeadKey));
                } catch (Exception e) {
                    log.warn("Failed to load HostFailState: " + e + " raw=" + raw, e);
                    }
                return !hostsToFailByType.isEmpty();
            }
        }

        /**
         * Internal method to convert a JSONArray from the SpawnDataStore to a list of hosts
         */
        private void loadHostsFromJSONArray(boolean deadFileSystem, JSONArray arr) throws JSONException {
            if (arr == null) {
                return;
            }
            Set<String> modified = deadFileSystem ? failFsOkay : failFsDead;
            for (int i = 0; i < arr.length(); i++) {
                modified.add(arr.getString(i));
            }
        }

        /**
         * Save the state to the SpawnDataStore
         */
        public void saveState() {
            try {
                synchronized (hostsToFailByType) {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put(filesystemOkayKey, new JSONArray(failFsDead));
                    jsonObject.put(filesystemDeadKey, new JSONArray(failFsOkay));
                    spawn.getSpawnDataStore().put(dataStoragePath, jsonObject.toString());
                }
            } catch (Exception e) {
                log.warn("Failed to save HostFailState: " + e, e);
                }
        }

        /**
         * Get the state of failure for a host
         *
         * @param hostId The host to check
         * @return ALIVE if the host is not being failed; otherwise, a description of the type of failure
         */
        public FailState getState(String hostId) {
            synchronized (hostsToFailByType) {
                if (failFsOkay.contains(hostId)) {
                    return FailState.FAILING_FS_DEAD;
                } else if (failFsDead.contains(hostId)) {
                    return FailState.FAILING_FS_OKAY;
                } else {
                    return FailState.ALIVE;
                }
            }
        }

    }

    public enum FailState {ALIVE, FAILING_FS_DEAD, FAILING_FS_OKAY}

}

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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.addthis.hydra.job.HostFailWorker.FailState;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.SpawnDataStoreKeys;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class storing the internal state of the host failure queue.
 * 
 * All changes are immediately saved to the SpawnDataStore.
 */
public class HostFailState {

    private static final Logger log = LoggerFactory.getLogger(HostFailState.class);

    private static final String filesystemDeadKey = "deadFs";
    private static final String filesystemOkayKey = "okayFs";
    private static final String filesystemFullKey = "fullFs";

    private final Spawn spawn;
    private final Set<String> failFsDead;
    private final Set<String> failFsOkay;
    private final Set<String> fsFull;
    private final Map<FailState, Set<String>> hostsToFailByType;

    public HostFailState(Spawn spawn) {
        this.spawn = spawn;
        failFsDead = new LinkedHashSet<>();
        failFsOkay = new LinkedHashSet<>();
        fsFull = new LinkedHashSet<>();
        hostsToFailByType = ImmutableMap.of(FailState.FAILING_FS_DEAD, failFsDead,
                FailState.DISK_FULL, fsFull, FailState.FAILING_FS_OKAY, failFsOkay);
    }

    /**
     * Add a new host to the failure queue
     *
     * @param hostId         The host id to add
     * @param failState      The state of the host being failed
     */
    public void putHost(String hostId, FailState failState) {

        synchronized (hostsToFailByType) {
            if (failFsDead.contains(hostId)) {
                log.info("Ignoring fs-okay failure of " + hostId + " because it is already being failed fs-dead");
                return;
            }
            switch (failState) {
            case FAILING_FS_DEAD:
                fsFull.remove(hostId);
                failFsOkay.remove(hostId);
                failFsDead.add(hostId);
                break;
            case FAILING_FS_OKAY:
                fsFull.remove(hostId);
                failFsOkay.add(hostId);
                break;
            case DISK_FULL:
                fsFull.add(hostId);
                failFsOkay.remove(hostId);
                break;
            default:
                log.warn("Unexcepted failState: {}", failState);
            }
            saveState();
        }
    }

    /**
     * Retrieve the next host to fail
     *
     * @return The uuid of the next host to fail, and whether the file system is dead. If the queue is empty, return null.
     */
    public Pair<String, FailState> nextHostToFail() {
        synchronized (hostsToFailByType) {
            String hostUuid = findFirstHost(failFsDead, false);
            if (hostUuid != null) {
                return Pair.of(hostUuid, FailState.FAILING_FS_DEAD);
            }
            hostUuid = findFirstHost(fsFull, true);
            if (hostUuid != null) {
                return Pair.of(hostUuid, FailState.DISK_FULL);
            }
            hostUuid = findFirstHost(failFsOkay, true);
            if (hostUuid != null) {
                return Pair.of(hostUuid, FailState.FAILING_FS_OKAY);
            }
            return null;
        }
    }

    private String findFirstHost(Set<String> hosts, boolean requireUp) {
        for (String hostUuid : hosts) {
            if (requireUp) {
                HostState host = spawn.getHostState(hostUuid);
                if (host != null && !host.isDead() && host.isUp()) {
                    return hostUuid;
                }
            } else {
                return hostUuid;
            }
        }
        return null;
    }

    /**
     * Cancel the failure for a host
     *
     * @param hostId The uuid to cancel
     */
    public void removeHost(String hostId) {
        synchronized (hostsToFailByType) {
            failFsDead.remove(hostId);
            failFsOkay.remove(hostId);
            fsFull.remove(hostId);
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
        String raw = spawnDataStore.get(SpawnDataStoreKeys.SPAWN_HOST_FAIL_WORKER_PATH);
        if (raw == null) {
            return false;
        }
        synchronized (hostsToFailByType) {
            try {
                JSONObject decoded = new JSONObject(raw);
                loadHostsFromJSONArray(failFsOkay, decoded.optJSONArray(filesystemOkayKey));
                log.info("Loaded hosts to fail fs-ok: {}", failFsOkay);
                loadHostsFromJSONArray(failFsDead, decoded.optJSONArray(filesystemDeadKey));
                log.info("Loaded hosts to fail fs-dead: {}", failFsDead);
                loadHostsFromJSONArray(fsFull, decoded.optJSONArray(filesystemFullKey));
                log.info("Loaded hosts to fail fs-full: {}", fsFull);
                return true;
            } catch (Exception e) {
                log.warn("Failed to load HostFailState: raw={}", raw, e);
                return false;
            }
        }
    }

    /**
     * Internal method to convert a JSONArray from the SpawnDataStore to a list of hosts
     */
    private void loadHostsFromJSONArray(Set<String> modified, JSONArray arr) throws JSONException {
        if (arr == null) {
            return;
        }
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
                jsonObject.put(filesystemDeadKey, new JSONArray(failFsDead));
                jsonObject.put(filesystemOkayKey, new JSONArray(failFsOkay));
                jsonObject.put(filesystemFullKey, new JSONArray(fsFull));
                spawn.getSpawnDataStore().put(SpawnDataStoreKeys.SPAWN_HOST_FAIL_WORKER_PATH, jsonObject.toString());
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
                return FailState.FAILING_FS_OKAY;
            } else if (failFsDead.contains(hostId)) {
                return FailState.FAILING_FS_DEAD;
            } else if (fsFull.contains(hostId)) {
                return FailState.DISK_FULL;
            } else {
                return FailState.ALIVE;
            }
        }
    }

}

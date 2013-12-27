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
package com.addthis.hydra.job.chores;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import com.addthis.basis.util.JitterClock;

import com.addthis.hydra.job.store.SpawnDataStore;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Watches a set of chores, carrying out actions when they finish
 */
public class ChoreWatcher {

    protected static Logger log = LoggerFactory.getLogger(ChoreWatcher.class);
    protected SpawnDataStore spawnDataStore;
    protected final int choreCleanupInterval;
    protected static final String CHOREWATCHER_BASE_PATH = "/chorewatcher";
    public final String CHOREWATCHER_PATH;
    protected final ExecutorService choreExecutor;
    protected ChoreMap choreMap = new ChoreMap();
    protected final ObjectMapper mapper = new ObjectMapper();
    private final String id;

    public ChoreWatcher(String id, SpawnDataStore spawnDataStore, ExecutorService choreExecutor, int choreCleanerInterval) throws Exception {
        this.choreCleanupInterval = choreCleanerInterval;
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                cleanChoreMap();
            }
        }, choreCleanerInterval, choreCleanerInterval);
        this.spawnDataStore = spawnDataStore;
        this.choreExecutor = choreExecutor;
        this.id = id;
        this.CHOREWATCHER_PATH = CHOREWATCHER_BASE_PATH + "/" + id;
        loadState();
    }

    protected void loadState() {
        try {
            String zkData = spawnDataStore.get(CHOREWATCHER_PATH);
            if (zkData != null && zkData.length() > 0) {
                choreMap = mapper.readValue(zkData, ChoreMap.class);
            }
            onAfterLoadState();
        } catch (IOException e) {
            log.warn("[ChoreWatcher] Unable to read chorewatcher state, exception: " + e, e);
            }
    }

    /**
     * subclasses may override to manipulate ChoreWatcher after
     * its state has been loaded
     */
    public void onAfterLoadState() {

    }

    private void cleanChoreMap() {
        for (Map.Entry<String, Chore> keyChoreEntry : choreMap.entrySet()) {
            Chore chore = keyChoreEntry.getValue();
            chore.lock();
            try {
                ArrayList<ChoreCondition> conditions = chore.getConditions();
                if (conditions != null && !conditions.isEmpty()) {
                    boolean done = true;
                    for (ChoreCondition condition : conditions) {
                        try {
                            if (!condition.checkCondition()) {
                                done = false;
                            }
                        } catch (Exception ex) {
                            done = false;
                        }
                    }
                    if (done) {
                        log.warn("[ChoreWatcher] finishing chore with conditions " + keyChoreEntry.getKey());
                        try {
                            finishChore(keyChoreEntry.getKey());
                        } catch (Exception ex) {
                            log.warn("[ChoreWatcher] Exception while finishing chore " + chore);
                        }
                        writeState();
                    }
                }
                if (chore.getStatus() == ChoreStatus.SUBMITTED && chore.getMaxRunTimeMillis() > 0) {
                    long currTime = JitterClock.globalTime();
                    if (currTime - chore.getStartTime() > chore.getMaxRunTimeMillis()) {
                        log.warn("[ChoreWatcher] Warning: chore has expired: " + chore);
                        chore.setExpired();
                        try {
                            finishChore(keyChoreEntry.getKey());
                        } catch (Exception ex) {
                            log.warn("[ChoreWatcher] Exception while finishing chore " + chore);
                        }
                        writeState();
                    }
                }
            } finally {
                chore.unlock();
            }
        }

    }

    public boolean addChore(Chore chore) throws Exception {
        log.warn("[ChoreWatcher] adding chore: " + chore);
        Chore previousChore = choreMap.putIfAbsent(chore.getKey(), chore);
        if (previousChore != null) {
            log.warn("[ChoreWatcher] warning: unable to add chore: " + chore + " because key already existed");
        }
        writeState();
        return previousChore == null;
    }

    public boolean finishChore(String key) throws Exception {
        Chore chore = removeChore(key);
        if (chore == null) {
            log.warn("[ChoreWatcher] Error: received finish message for null chore " + key);
            return false;
        }
        chore.lock();
        try {
            if (chore.getStatus() == ChoreStatus.SUBMITTED) {
                chore.setFinished();
            }
            switch (chore.getStatus()) {
                case FINISHED:
                    log.warn("[ChoreWatcher] Carrying out on-finish actions for " + key);
                    chore.doFinish(choreExecutor);
                    break;
                case EXPIRED:
                case ERRORED:
                    log.warn("[ChoreWatcher] Carrying out on-error actions for " + key);
                    chore.doError(choreExecutor);
                    break;
                default:
                    log.warn("[ChoreWatcher] Unexpected status: " + chore.getStatus());
                    break;
            }

        } finally {
            chore.unlock();
        }

        return true;
    }

    public boolean errorChore(String key) {
        Chore chore = removeChore(key);
        if (chore == null) {
            log.warn("[ChoreWatcher] Error: received error message for null chore " + key);
            return false;
        }
        chore.lock();
        try {
            chore.setErrored();
            chore.doError(choreExecutor);
        } finally {
            chore.unlock();
        }

        return true;
    }

    public boolean hasChore(String key) {
        return choreMap.containsKey(key);
    }

    public Chore removeChore(String key) {
        Chore chore;
        log.warn("[ChoreWatcher] Removing chore " + key);
        chore = choreMap.remove(key);
        if (chore != null) {
            writeState();
        } else {
            log.warn("[ChoreWatcher] warning: attempted to remove non-existent choreKey " + key);
        }
        return chore;
    }

    protected void writeState() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            spawnDataStore.put(CHOREWATCHER_PATH, new String(mapper.writeValueAsBytes(choreMap)));
        } catch (Exception e) {
            log.warn("[ChoreWatcher] Unable to write chorewatcher state, exception: " + e, e);
            }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ChoreWatcher");
        sb.append(", choreCleanupInterval=").append(choreCleanupInterval);
        sb.append(", CHOREWATCHER_PATH='").append(CHOREWATCHER_PATH).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

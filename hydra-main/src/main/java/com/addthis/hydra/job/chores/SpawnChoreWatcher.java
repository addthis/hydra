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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.store.SpawnDataStore;



/**
 * Watches a set of chores, carrying out actions when they finish
 */
public class SpawnChoreWatcher extends ChoreWatcher {

    private Spawn spawn;


    public SpawnChoreWatcher(Spawn spawn, String id, SpawnDataStore spawnDataStore, ExecutorService choreExecutor, int choreCleanerInterval) throws Exception {
        super(id, spawnDataStore, choreExecutor, choreCleanerInterval);
        this.spawn = spawn;
    }

    @Override
    public void onAfterLoadState() {
        for (Map.Entry<String, Chore> keyChoreEntry : choreMap.entrySet()) {
            Chore chore = keyChoreEntry.getValue();
            chore.setStartTime(System.currentTimeMillis());
            addSpawnToChoreConditions(chore.getConditions());
            addSpawnToChoreActions(chore.getOnError());
            addSpawnToChoreActions(chore.getOnFinish());
        }
    }

    private void addSpawnToChoreConditions(List<ChoreCondition> conditions) {
        if (conditions == null) {
            return;
        }
        for (ChoreCondition condition : conditions) {
            if (condition instanceof SpawnCondition) {
                ((SpawnCondition) condition).setSpawn(spawn);
            }
        }
    }

    private void addSpawnToChoreActions(List<ChoreAction> errorActions) {
        if (errorActions == null) {
            return;
        }
        for (ChoreAction choreAction : errorActions) {
            if (choreAction instanceof SpawnAction) {
                ((SpawnAction) choreAction).setSpawn(spawn);
            }
        }
    }
}

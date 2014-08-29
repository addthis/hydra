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
package com.addthis.hydra.job.entity;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_COMMAND_PATH;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.spawn.Spawn;

public class JobCommandManager extends AbstractJobEntityManager<JobCommand> {

    public JobCommandManager(Spawn spawn) throws Exception {
        super(spawn, JobCommand.class, SPAWN_COMMON_COMMAND_PATH);
    }

    @Override
    protected Job findDependentJob(Spawn spawn, String entityKey) {
        for (Job job : spawn.listJobs()) {
            if (job.getCommand() != null && job.getCommand().equals(entityKey)) {
                return job;
            }
        }
        return null;
    }

}

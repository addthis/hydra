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
package com.addthis.hydra.job.spawn;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SpawnUtils {
    public static Map<String, Job> getJobsMapFromSpawnState(SpawnState spawnState) {
        return ImmutableMap.copyOf(spawnState.jobs);
    }

    public static Map<String, JobMacro> getMacroMapFromMacroManager(JobEntityManager<JobMacro> jobMacroManager) {
        ImmutableMap.Builder<String, JobMacro> builder = ImmutableMap.builder();

        for (String macroName : jobMacroManager.getKeys()) {
            builder.put(macroName, jobMacroManager.getEntity(macroName));
        }

        return builder.build();
    }
}

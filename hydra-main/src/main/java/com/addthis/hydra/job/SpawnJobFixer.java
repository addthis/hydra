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

import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.mq.JobKey;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * A class in charge of performing common fixes when tasks go into error state.
 */
public class SpawnJobFixer {

    private static Logger log = LoggerFactory.getLogger(SpawnJobFixer.class);
    private final Spawn spawn;
    private static final long recentlyFixedTaskTime = Parameter.longValue("spawn.task.fix.time", 60_000);
    private final Cache<JobKey, Boolean> recentlyFixedTaskCache = CacheBuilder.newBuilder().expireAfterWrite(recentlyFixedTaskTime, TimeUnit.MILLISECONDS).build();

    private final ImmutableSet<Integer> fixDirErrorCodes = ImmutableSet.copyOf(new Integer[]{JobTaskErrorCode.SWAP_FAILURE, JobTaskErrorCode.EXIT_DIR_ERROR, JobTaskErrorCode.HOST_FAILED});

    public SpawnJobFixer(Spawn spawn) {
        this.spawn = spawn;
    }

    public void fixTask(Job job, JobTask task, int errorCode) throws Exception {
        if (job == null || task == null) {
            log.warn("fixer received null job/task; ignoring");
            return;
        }
        if (fixDirErrorCodes.contains(errorCode) && recentlyFixedTaskCache.getIfPresent(task.getJobKey()) == null) {
            log.warn("fixer attempting to fix dirs for task " + task.getJobKey());
            spawn.fixTaskDir(job.getId(), task.getTaskID(), true);
            job.setTaskState(task, JobTaskState.IDLE, true);
            recentlyFixedTaskCache.put(task.getJobKey(), true);
            if (errorCode != JobTaskErrorCode.HOST_FAILED) {
                spawn.scheduleTask(job, task, null);
            }
        } else {
            job.errorTask(task, errorCode);
        }
    }

}

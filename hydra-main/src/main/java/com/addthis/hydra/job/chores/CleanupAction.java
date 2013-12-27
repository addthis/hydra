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


import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.mq.JobKey;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class CleanupAction extends SpawnAction {

    private static Logger log = LoggerFactory.getLogger(CleanupAction.class);

    private final JobKey taskKey;
    private final String targetHostUUID;
    private final String sourceHostUUID;

    public CleanupAction() {
        super(null);
        this.taskKey = null;
        this.targetHostUUID = "";
        this.sourceHostUUID = "";
    }

    public CleanupAction(Spawn spawn, JobKey taskKey, String targetHostUUID, String sourceHostUUID) {
        super(spawn);
        this.taskKey = taskKey;
        this.targetHostUUID = targetHostUUID;
        this.sourceHostUUID = sourceHostUUID;
    }

    @Override
    public void run() {
        if (taskKey == null) {
            log.warn("[task.mover] received null taskkey; exiting");
            return;
        }
        Job job = spawn.getJob(taskKey);
        JobTask task = job.getTask(taskKey.getNodeNumber());
        if (task == null) {
            log.warn("[task.mover] failed to find job or task for: " + taskKey);
            return;
        }
        log.warn("[task.mover] sending delete message to target host");
        try {
            spawn.deleteJob(task.getJobUUID(), targetHostUUID, task.getTaskID(), true);
            job.setTaskState(task, JobTaskState.IDLE);
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    public JobKey getTaskKey() {
        return taskKey;
    }

    public String getTargetHostUUID() {
        return targetHostUUID;
    }

    public String getSourceHostUUID() {
        return sourceHostUUID;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CleanupChore");
        sb.append("{jobkey=").append(taskKey);
        sb.append(", target=").append(targetHostUUID);
        sb.append(", source=").append(sourceHostUUID);
        sb.append('}');
        return sb.toString();
    }
}

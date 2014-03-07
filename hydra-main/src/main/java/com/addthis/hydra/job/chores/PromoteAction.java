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
public class PromoteAction extends SpawnAction {

    private static Logger log = LoggerFactory.getLogger(PromoteAction.class);

    private final JobKey taskKey;
    private final String targetHostUUID;
    private final String sourceHostUUID;
    private final boolean kickOnComplete;
    private final boolean deleteReplica;
    private final boolean ignoreLive;

    public PromoteAction() {
        super(null);
        this.taskKey = null;
        this.targetHostUUID = "";
        this.sourceHostUUID = "";
        this.kickOnComplete = false;
        this.deleteReplica = true;
        this.ignoreLive = false;
    }

    public PromoteAction(Spawn spawn, JobKey taskKey, String targetHostUUID, String sourceHostUUID, boolean kickOnComplete, boolean deleteReplica, boolean ignoreLive) {
        super(spawn);
        this.taskKey = taskKey;
        this.targetHostUUID = targetHostUUID;
        this.sourceHostUUID = sourceHostUUID;
        this.kickOnComplete = kickOnComplete;
        this.deleteReplica = deleteReplica;
        this.ignoreLive = ignoreLive;
    }

    @Override
    public void run() {
        if (!spawn.checkStatusForMove(targetHostUUID) || (!ignoreLive && !spawn.checkStatusForMove(sourceHostUUID))) {
            log.warn("[task.mover] bad status during execution");
            return;
        }
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
        try {
            swapAndDeleteOldCopy();
            if (kickOnComplete) {
                job.setTaskState(task, JobTaskState.QUEUED, true);
                spawn.addToTaskQueue(taskKey, false, true);
            } else {
                job.setTaskState(task, JobTaskState.IDLE, true);
            }
        } catch (Exception ex) {
            log.warn("[task.mover] exception during promoting", ex);
            }
    }

    private void swapAndDeleteOldCopy() throws Exception {
        Job job = spawn.getJob(taskKey);
        JobTask task = job.getTask(taskKey.getNodeNumber());
        log.warn("[task.mover] promoting " + task.getJobKey() + " on " + targetHostUUID + "; original was on " + sourceHostUUID);
        String oldHostUUID = task.getHostUUID();
        task.setHostUUID(targetHostUUID);
        if (!oldHostUUID.equals(sourceHostUUID)) {
            task.replaceReplica(sourceHostUUID, oldHostUUID);
        }
        spawn.deleteTask(taskKey.getJobUuid(), sourceHostUUID, taskKey.getNodeNumber(), false);
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
        sb.append("PromoteChore");
        sb.append("{jobkey=").append(taskKey);
        sb.append(", target=").append(targetHostUUID);
        sb.append(", source=").append(sourceHostUUID);
        sb.append('}');
        return sb.toString();
    }
}

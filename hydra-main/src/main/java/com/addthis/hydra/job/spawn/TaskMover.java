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

import com.addthis.basis.util.LessStrings;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskErrorCode;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.mq.CommandTaskReplicate;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.ReplicaTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class moves a task from a source host to a target host.
 * If the target host already had a replica of the task, that
 * replica is removed so the task will make a new replica somewhere
 * else.
 */
class TaskMover {
    private static final Logger log = LoggerFactory.getLogger(TaskMover.class);

    private HostState targetHost;
    private Job job;
    private JobTask task;
    private boolean kickOnComplete;
    private boolean isMigration;

    private final JobKey taskKey;
    private final String targetHostUUID;
    private final String sourceHostUUID;
    private final Spawn spawn;
    private final HostManager hostManager;

    TaskMover(Spawn spawn, HostManager hostManager, JobKey taskKey, String targetHostUUID, String sourceHostUUID) {
        this.spawn = spawn;
        this.hostManager = hostManager;
        this.taskKey = taskKey;
        this.targetHostUUID = targetHostUUID;
        this.sourceHostUUID = sourceHostUUID;
    }

    public void setMigration(boolean isMigration) {
        this.isMigration = isMigration;
    }

    public String choreWatcherKey() {
        return targetHostUUID + "&&&" + taskKey;
    }

    public boolean execute() {
        targetHost = hostManager.getHostState(targetHostUUID);
        if (taskKey == null || !spawn.checkStatusForMove(targetHostUUID) || !spawn.checkStatusForMove(sourceHostUUID)) {
            log.warn("[task.mover] erroneous input; terminating for: " + taskKey);
            return false;
        }
        job = spawn.getJob(taskKey);
        task = job.getTask(taskKey.getNodeNumber());
        if (task == null) {
            log.warn("[task.mover] failed to find job or task for: " + taskKey);
            return false;
        }
        HostState liveHost = hostManager.getHostState(task.getHostUUID());
        if (liveHost == null || !liveHost.hasLive(task.getJobKey())) {
            log.warn("[task.mover] failed to find live task for: " + taskKey);
            spawn.fixTaskDir(taskKey.getJobUuid(), taskKey.getNodeNumber(), false, false);
            return false;
        }
        if (!task.getHostUUID().equals(sourceHostUUID) && !task.hasReplicaOnHost(sourceHostUUID)) {
            log.warn("[task.mover] failed because the task does not have a copy on the specified source: " + taskKey);
            return false;
        }
        if (task.getAllTaskHosts().contains(targetHostUUID) || targetHost.hasLive(taskKey)) {
            log.warn("[task.mover] cannot move onto a host with an existing version of task: " + taskKey);
            return false;
        }
        if (!targetHost.getMinionTypes().contains(job.getMinionType())) {
            log.warn("[task.mover] cannot move onto a host that lacks the appropriate minion type: " + taskKey);
            return false;
        }
        // If the task was rebalanced out of queued state, kick it again when the rebalance completes.
        kickOnComplete = task.getState().isQueuedState();
        if (!spawn.prepareTaskStatesForRebalance(job, task, isMigration)) {
            log.warn("[task.mover] couldn't set task states; terminating for: " + taskKey);
            return false;
        }
        // Swap to the lightest host to run the rsync operation, assuming swapping is allowed for this job.
        HostState lightestExistingHost = spawn.taskQueuesByPriority.findBestHostToRunTask(
                spawn.getHealthyHostStatesHousingTask(task, !job.getDontAutoBalanceMe()),
                false);
        if (lightestExistingHost != null && !lightestExistingHost.getHostUuid().equals(task.getHostUUID())) {
            spawn.swapTask(task, lightestExistingHost.getHostUuid(), false);
        }
        try {
            task.setRebalanceSource(sourceHostUUID);
            task.setRebalanceTarget(targetHostUUID);
            startReplicate();
            spawn.taskQueuesByPriority.markHostTaskActive(task.getHostUUID());
            spawn.queueJobTaskUpdateEvent(job);
            return true;
        } catch (Exception ex) {
            log.warn("[task.mover] exception during replicate initiation; terminating for task: " + taskKey, ex);
            task.setErrorCode(JobTaskErrorCode.EXIT_REPLICATE_FAILURE);
            task.setState(JobTaskState.ERROR);
            spawn.queueJobTaskUpdateEvent(job);
            return false;
        }
    }

    private void startReplicate() throws Exception {
        ReplicaTarget[] target = { new ReplicaTarget(targetHostUUID,
                                                     targetHost.getHost(),
                                                     targetHost.getUser(),
                                                     targetHost.getPath())
        };
        JobCommand jobcmd = spawn.getJobCommandManager().getEntity(job.getCommand());
        CommandTaskReplicate replicate = new CommandTaskReplicate(task.getHostUUID(),
                                                                  task.getJobUUID(),
                                                                  task.getTaskID(),
                                                                  target,
                                                                  LessStrings.join(jobcmd.getCommand(), " "),
                                                                  choreWatcherKey(),
                                                                  true,
                                                                  kickOnComplete);
        replicate.setRebalanceSource(sourceHostUUID);
        replicate.setRebalanceTarget(targetHostUUID);
        spawn.sendControlMessage(replicate);
        log.warn("[task.mover] replicating job/task {} from {} onto host {}",
                 task.getJobKey(), sourceHostUUID, targetHostUUID);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TaskMover");
        sb.append("{taskKey=").append(taskKey);
        sb.append(", targetHostUUID='").append(targetHostUUID).append('\'');
        sb.append(", sourceHostUUID='").append(sourceHostUUID).append('\'');
        sb.append(", job=").append(job);
        sb.append(", task=").append(task);
        sb.append(", kickOnComplete=").append(kickOnComplete);
        sb.append('}');
        return sb.toString();
    }
}

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

import java.util.HashMap;
import java.util.Map;

import com.addthis.basis.util.JitterClock;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;

class UpdateEventRunnable implements Runnable {

    private final Spawn spawn;
    private final Map<String, Long> events = new HashMap<>();

    public UpdateEventRunnable(Spawn spawn) {
        this.spawn = spawn;
    }

    @Override
    public void run() {
        int slotsAvailable = 0;
        int slotsTotal = 0;
        int jobsTotal = 0;
        int jobsHung = 0;
        int jobsRunning = 0;
        int jobsScheduled = 0;
        int jobsErrored = 0;
        int tasksTotal = 0;
        int tasksProcessing = 0;
        int tasksReplicating = 0;
        int tasksBackingUp = 0;
        int tasksBusy = 0;
        int tasksRebalancing = 0;
        int tasksErrored = 0;
        int tasksQueued = 0;
        int taskQueuedNoSlot = 0;
        long files = 0;
        long bytes = 0;
        spawn.jobLock.lock();
        try {
            for (HostState hostState : spawn.hostManager.monitored.values()) {
                if (hostState.isUp()) {
                    slotsAvailable += hostState.getAvailableTaskSlots();
                    slotsTotal += hostState.getMaxTaskSlots();
                }
            }
            for (Job job : spawn.spawnState.jobs.values()) {
                jobsTotal++;
                for (JobTask jn : job.getCopyOfTasks()) {
                    tasksTotal++;
                    switch (jn.getState()) {
                        case ALLOCATED:
                            tasksBusy++;
                            break;
                        case BUSY:
                            tasksProcessing++;
                            tasksBusy++;
                            break;
                        case BACKUP:
                            tasksBackingUp++;
                            tasksBusy++;
                            break;
                        case REPLICATE:
                            tasksReplicating++;
                            tasksBusy++;
                            break;
                        case REBALANCE:
                            tasksRebalancing++;
                            tasksBusy++;
                            break;
                        case REVERT:
                            tasksBusy++;
                            break;
                        case SWAPPING:
                            tasksBusy++;
                            break;
                        case MIGRATING:
                            tasksBusy++;
                            break;
                        case FULL_REPLICATE:
                            tasksReplicating++;
                            tasksBusy++;
                            break;
                        case ERROR:
                            tasksErrored++;
                            break;
                        case IDLE:
                            break;
                        case QUEUED:
                            tasksQueued++;
                            break;
                        case QUEUED_HOST_UNAVAIL:
                            tasksQueued++;
                            break;
                        case QUEUED_NO_SLOT:
                            tasksQueued++;
                            taskQueuedNoSlot++;
                            break;
                    }
                    files += jn.getFileCount();
                    bytes += jn.getByteCount();
                }
                switch (job.getState()) {
                    case IDLE:
                        break;
                    case RUNNING:
                        jobsRunning++;
                        if (job.getStartTime() != null && job.getMaxRunTime() != null &&
                            (JitterClock.globalTime() - job.getStartTime() > job.getMaxRunTime() * 2)) {
                            jobsHung++;
                        }
                        break;
                    case SCHEDULED:
                        jobsScheduled++;
                        break;
                }
                if (job.getState() == JobState.ERROR) {
                    jobsErrored++;
                }
            }
        } finally {
            spawn.jobLock.unlock();
        }
        long diskUsed = 0;
        long diskCapacity = 0;
        for (HostState host : spawn.hostManager.getLiveHosts(null)) {
            diskUsed += host.getUsed().getDisk();
            diskCapacity += host.getMax().getDisk();
        }
        float diskAvailable = ((float) diskUsed) / ((float) diskCapacity);
        events.clear();
        events.put("time", System.currentTimeMillis());
        events.put("hosts", (long) spawn.hostManager.monitored.size());
        events.put("commands", (long) spawn.getJobCommandManager().size());
        events.put("macros", (long) spawn.getJobMacroManager().size());
        events.put("jobs", (long) spawn.spawnState.jobs.size());
        events.put("jobs_running", (long) jobsRunning);
        events.put("jobs_scheduled", (long) jobsScheduled);
        events.put("jobs_errored", (long) jobsErrored);
        events.put("jobs_hung", (long) jobsHung);
        events.put("jobs_total", (long) jobsTotal);
        events.put("tasks_busy", (long) tasksBusy);
        events.put("tasks_queued", (long) tasksQueued);
        events.put("tasks_queued_no_slot", (long) taskQueuedNoSlot);
        events.put("tasks_errored", (long) tasksErrored);
        events.put("tasks_total", (long) tasksTotal);
        events.put("slots_available", (long) slotsAvailable);
        events.put("slots_total", (long) slotsTotal);
        events.put("files", files);
        events.put("bytes", bytes);
        events.put("disk_used", diskUsed);
        events.put("disk_capacity", diskCapacity);
        spawn.spawnFormattedLogger.periodicState(events);
        SpawnMetrics.totalTaskCount.set(tasksTotal);
        SpawnMetrics.runningTaskCount.set(tasksBusy);
        SpawnMetrics.queuedTaskCount.set(tasksQueued);
        SpawnMetrics.queuedTaskNoSlotCount.set(taskQueuedNoSlot);
        SpawnMetrics.failTaskCount.set(tasksErrored);
        SpawnMetrics.totalJobCount.set(jobsTotal);
        SpawnMetrics.processingTaskCount.set(tasksProcessing);
        SpawnMetrics.replicatingTaskCount.set(tasksReplicating);
        SpawnMetrics.backingUpTaskCount.set(tasksBackingUp);
        SpawnMetrics.runningJobCount.set(jobsRunning);
        SpawnMetrics.rebalancingTaskCount.set(tasksRebalancing);
        SpawnMetrics.queuedJobCount.set(jobsScheduled);
        SpawnMetrics.failJobCount.set(jobsErrored);
        SpawnMetrics.hungJobCount.set(jobsHung);
        SpawnMetrics.diskAvailablePercent.set(diskAvailable);
        SpawnMetrics.availableSlotCount.set(slotsAvailable);
        SpawnMetrics.totalSlotCount.set(slotsTotal);
    }
}

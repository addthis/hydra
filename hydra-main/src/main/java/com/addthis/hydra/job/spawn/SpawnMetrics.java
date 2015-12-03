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

import java.util.concurrent.TimeUnit;

import com.addthis.hydra.util.SettableGauge;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

import static com.addthis.hydra.util.SettableGauge.newSettableGauge;

public final class SpawnMetrics {
    static final SettableGauge<Integer> processingTaskCount = newSettableGauge(Spawn.class, "processingTasks", 0);
    static final SettableGauge<Integer> replicatingTaskCount = newSettableGauge(Spawn.class, "replicatingTasks", 0);
    static final SettableGauge<Integer> backingUpTaskCount = newSettableGauge(Spawn.class, "backingUpTasks", 0);
    static final SettableGauge<Integer> runningTaskCount = newSettableGauge(Spawn.class, "runningTasks", 0);
    static final SettableGauge<Integer> queuedTaskCount = newSettableGauge(Spawn.class, "queuedTasks", 0);
    static final SettableGauge<Integer> queuedTaskNoSlotCount = newSettableGauge(Spawn.class, "queuedTasksNoSlot", 0);
    static final SettableGauge<Integer> failTaskCount = newSettableGauge(Spawn.class, "failedTasks", 0);
    static final SettableGauge<Integer> rebalancingTaskCount = newSettableGauge(Spawn.class, "rebalancingTasks", 0);
    static final SettableGauge<Integer> totalTaskCount = newSettableGauge(Spawn.class, "totalTasks", 0);
    static final SettableGauge<Integer> runningJobCount = newSettableGauge(Spawn.class, "runningJobs", 0);
    static final SettableGauge<Integer> queuedJobCount = newSettableGauge(Spawn.class, "queuedJobs", 0);
    static final SettableGauge<Integer> failJobCount = newSettableGauge(Spawn.class, "failedJobs", 0);
    static final SettableGauge<Integer> hungJobCount = newSettableGauge(Spawn.class, "hungJobs", 0);
    static final SettableGauge<Integer> totalJobCount = newSettableGauge(Spawn.class, "totalJobs", 0);
    static final SettableGauge<Float> diskAvailablePercent = newSettableGauge(Spawn.class, "diskAvailable", 0.0f);
    static final SettableGauge<Integer> availableSlotCount = newSettableGauge(Spawn.class, "availableSlots", 0);
    static final SettableGauge<Integer> totalSlotCount = newSettableGauge(Spawn.class, "totalSlots", 0);

    static final Meter tasksStartedPerHour =
            Metrics.newMeter(Spawn.class, "tasksStartedPerHour", "tasksStartedPerHour", TimeUnit.HOURS);
    static final Meter tasksCompletedPerHour =
            Metrics.newMeter(Spawn.class, "tasksCompletedPerHour", "tasksCompletedPerHour", TimeUnit.HOURS);
    static final Meter jobsStartedPerHour =
            Metrics.newMeter(Spawn.class, "jobsStartedPerHour", "jobsStartedPerHour", TimeUnit.HOURS);
    static final Meter jobsCompletedPerHour =
            Metrics.newMeter(Spawn.class, "jobsCompletedPerHour", "jobsCompletedPerHour", TimeUnit.HOURS);
    static final Meter jobTaskUpdateHeartbeatSuccessMeter =
            Metrics.newMeter(Spawn.class, "jobTaskUpdateHeartbeatSuccess",
                             "jobTaskUpdateHeartbeatSuccess", TimeUnit.MINUTES);

    static final Counter quiesceCount = Metrics.newCounter(Spawn.class, "quiesced");
    static final Counter nonConsumingClientDropCounter = Metrics.newCounter(Spawn.class, "clientDrops");
    static final Counter nonHostTaskMessageCounter = Metrics.newCounter(Spawn.class, "nonHostTaskMessage");
    static final Counter jobTaskUpdateHeartbeatFailureCounter =
            Metrics.newCounter(Spawn.class, "jobTaskUpdateHeartbeatFailure");

    private SpawnMetrics() {}
}

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

package com.addthis.hydra.query.aggregate;

import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.JitterClock;

import com.addthis.hydra.data.query.Query;

class StragglerCheckTask implements Runnable {

    private final AggregateHandle aggregateHandle;

    public StragglerCheckTask(AggregateHandle aggregateHandle) {
        this.aggregateHandle = aggregateHandle;
    }

    @Override
    public void run() {
        if (aggregateHandle.done.get() || aggregateHandle.canceled) {
            return;
        }
        if (AggregateConfig.useStdDevStragglers) {
            checkForStragglersStdDev();
        } else {
            checkForStragglersMeans();
        }
    }

    void checkForStragglersStdDev() {
        AggregateHandle handle = aggregateHandle;
        Query query = aggregateHandle.query;

        int totalTasks = handle.totalTasks;
        int numRemaining = totalTasks - handle.completed;
        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(AggregateConfig.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
            }
        } else if ((numRemaining <= tasksDoneCutoff) &&
                   (elapsedTime > handle.getStdDevsAwayRuntime(AggregateConfig.MULTIPLE_STD_DEVS))) {
            if (MeshSourceAggregator.log.isTraceEnabled()) {
                MeshSourceAggregator.log.trace("Running stragglers for query: {}", query.uuid());
                MeshSourceAggregator.log.trace(
                        "numRemaining: {} taskDoneCutoff: {} deltaTime: {} {} stdDevsAway: {} Mean runtime: {}",
                        numRemaining, tasksDoneCutoff, elapsedTime, AggregateConfig.MULTIPLE_STD_DEVS,
                        handle.getStdDevsAwayRuntime(AggregateConfig.MULTIPLE_STD_DEVS), handle.getMeanRuntime());
            }
            aggregateHandle.handleStragglers();
        }
        aggregateHandle.meshSourceAggregator.executor.schedule(
                this, AggregateConfig.stragglerCheckPeriod, TimeUnit.MILLISECONDS);
    }

    void checkForStragglersMeans() {
        AggregateHandle handle = aggregateHandle;
        Query query = handle.query;

        int totalTasks = handle.totalTasks;
        int numRemaining = totalTasks - handle.completed;

        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(AggregateConfig.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
        double timeCutoff = AggregateConfig.stragglerCheckMeanRuntimeFactor * handle.getMeanRuntime();
        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
            }
        } else if ((numRemaining <= tasksDoneCutoff) && (elapsedTime > timeCutoff)) {
            handle.handleStragglers();
        }
    }
}

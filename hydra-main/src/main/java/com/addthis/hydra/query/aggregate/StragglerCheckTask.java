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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StragglerCheckTask implements Runnable {

    static final Logger log = LoggerFactory.getLogger(StragglerCheckTask.class);

    private final MeshSourceAggregator sourceAggregator;

    public StragglerCheckTask(MeshSourceAggregator sourceAggregator) {
        this.sourceAggregator = sourceAggregator;
    }

    @Override
    public void run() {
        if (sourceAggregator.queryPromise.isDone()) {
            return;
        }
        try {
            if (AggregateConfig.useStdDevStragglers) {
                checkForStragglersStdDev();
            } else {
                checkForStragglersMeans();
            }
        } catch (Exception ex) {
            log.warn("Unexpected exception found while doing Straggler checks", ex);
        }
    }

    void checkForStragglersStdDev() {
        Query query = sourceAggregator.query;

        int totalTasks = sourceAggregator.totalTasks;
        int numRemaining = totalTasks - sourceAggregator.completed;
        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(AggregateConfig.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - sourceAggregator.startTime;
        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
            }
        } else if ((numRemaining <= tasksDoneCutoff) &&
                   (elapsedTime > getStdDevsAwayRuntime(AggregateConfig.MULTIPLE_STD_DEVS))) {
            if (MeshSourceAggregator.log.isTraceEnabled()) {
                MeshSourceAggregator.log.trace("Running stragglers for query: {}", query.uuid());
                MeshSourceAggregator.log.trace(
                        "numRemaining: {} taskDoneCutoff: {} deltaTime: {} {} stdDevsAway: {} Mean runtime: {}",
                        numRemaining, tasksDoneCutoff, elapsedTime, AggregateConfig.MULTIPLE_STD_DEVS,
                        getStdDevsAwayRuntime(AggregateConfig.MULTIPLE_STD_DEVS), getMeanRuntime());
            }
            handleStragglers();
        }
        sourceAggregator.executor.schedule(
                this, AggregateConfig.stragglerCheckPeriod, TimeUnit.MILLISECONDS);
    }

    void checkForStragglersMeans() {
        int totalTasks = sourceAggregator.totalTasks;
        int numRemaining = totalTasks - sourceAggregator.completed;

        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(AggregateConfig.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - sourceAggregator.startTime;
        double timeCutoff = AggregateConfig.stragglerCheckMeanRuntimeFactor * getMeanRuntime();
        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || sourceAggregator.query.isTraced()) {
                Query.emitTrace("Straggler checker for " + sourceAggregator.query.uuid() + " detected query done. Exiting.");
            }
        } else if ((numRemaining <= tasksDoneCutoff) && (elapsedTime > timeCutoff)) {
            handleStragglers();
        }
    }

    private long[] getRuntimes() {
        long[] runtimes = new long[sourceAggregator.completed];
        long startTime = sourceAggregator.startTime;
        int runtimesIndex = 0;
        for (QueryTaskSource taskSource : sourceAggregator.taskSources) {
            if (taskSource.complete()) {
                if (taskSource.endTime > 0) {
                    runtimes[runtimesIndex] = taskSource.endTime - startTime;
                } else { // special case for allowPartial tasks
                    runtimes[runtimesIndex] = -1;
                }
                runtimesIndex += 1;
                if (runtimesIndex >= runtimes.length) {
                    break;
                }
            }
        }
        return runtimes;
    }

    double getMeanRuntime() {
        double sum = 0;
        int realCompletes = 0;
        for (long runtime : getRuntimes()) {
            if (runtime >= 0) {
                sum += runtime;
                realCompletes += 1;
            }
        }
        if (realCompletes > 0) {
            sum /= realCompletes;
        }
        return sum;
    }

    /**
     * Calculates the point in time multipleStdDevs away from the mean
     *
     * @param multipleStdDevs the number of times to add the stadard deviation
     * @return the mean plus multipleStdDevs of the standard deviation
     */
    double getStdDevsAwayRuntime(double multipleStdDevs) {
        double x = 0;
        double x2 = 0;
        int realCompletes = 0;
        double stdDevsAway = 0;

        for (long runtime : getRuntimes()) {
            if (runtime >= 0) {
                x += runtime;
                x2 += runtime * runtime;
                realCompletes += 1;
            }
        }

        if (realCompletes > 0) {
            double mean = x / realCompletes;

            // This is an approximation. With large n we can ignore the -1 in the n-1 in the denominator of the
            // variance. However, to avoid division by 0 we ignore it
            double variance = (x2 - 2 * x * mean + realCompletes * mean * mean) / realCompletes;
            stdDevsAway = mean + multipleStdDevs * Math.sqrt(variance);
        }

        return stdDevsAway;
    }


    public void handleStragglers() {
        for (QueryTaskSource taskSource : sourceAggregator.taskSources) {
            if (taskSource.oneHasResponded() || (taskSource.options.length == 0)) {
                continue;
            }
            for (QueryTaskSourceOption option : taskSource.options) {
                if (!option.isActive()) {
                    if (option.tryActivate(sourceAggregator.meshy, sourceAggregator.queryOptions)) {
                        AggregateConfig.totalStragglerCheckerRequests.inc();
                        if (MeshSourceAggregator.log.isDebugEnabled() || sourceAggregator.query.isTraced()) {
                            Query.emitTrace("Straggler detected for " + sourceAggregator.query.uuid() + " sending duplicate query to host: "
                                            + option.queryReference.getHostUUID());
                        }
                        break;
                    }
                }
            }
        }
    }
}

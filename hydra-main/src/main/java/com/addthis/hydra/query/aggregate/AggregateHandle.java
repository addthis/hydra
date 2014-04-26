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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.source.QueryConsumer;
import com.addthis.hydra.data.query.source.QueryHandle;

import io.netty.util.concurrent.EventExecutor;

class AggregateHandle implements QueryHandle, DataChannelOutput {

    final MeshSourceAggregator meshSourceAggregator;
    final QueryConsumer consumer;
    final AtomicBoolean done = new AtomicBoolean(false);
    final Query query;
    final List<Long> runtimes;
    final int totalTasks;
    final QueryTaskSource[] taskSources;

    int completed;
    boolean canceled = false;
    DataChannelError error;

    AggregateHandle(MeshSourceAggregator meshSourceAggregator, final Query query,
            final DataChannelOutput consumer, QueryTaskSource[] taskSources) {
        this.meshSourceAggregator = meshSourceAggregator;
        this.query = query;
        this.consumer = new QueryConsumer(consumer);
        this.totalTasks = meshSourceAggregator.totalTasks;
        this.runtimes = new ArrayList<>(meshSourceAggregator.totalTasks);
        this.taskSources = taskSources;
    }

    public long getStartTime() {
        return meshSourceAggregator.startTime;
    }

    @Override
    public void cancel(final String message) //closes connections to workers and consumer
    {
        EventExecutor executor = meshSourceAggregator.executor;
        if (executor.inEventLoop()) {
            invokeCancel(message);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    invokeCancel(message);
                }
            });
        }
    }

    private void invokeCancel(String message) {
        // only cancel one time here
        if (!canceled) {
            canceled = true;
            MeshSourceAggregator.log.warn("[aggregate.handle] cancel called. error:{}", error.toString());
            stopSources(message);
            error(new DataChannelError(message));
        }
    }

    void stopSources(String message) {
        //Close connections to workers and call done if needed
        for (QueryTaskSource taskSource : taskSources) {
            taskSource.cancelAllActiveOptions(message);
        }
    }

    @Override
    public void send(Bundle bundle) throws DataChannelError {
        try {
            consumer.send(bundle);
        } catch (Exception e) {
            DataChannelError de = DataChannelError.promote(e);
            error(de);
            throw de;
        }
    }

    @Override
    public void sendComplete() {
        EventExecutor executor = meshSourceAggregator.executor;
        if (executor.inEventLoop()) {
            done();
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    done();
                }
            });
        }
    }

    @Override
    public void sourceError(final DataChannelError er) {
        EventExecutor executor = meshSourceAggregator.executor;
        if (executor.inEventLoop()) {
            error(er);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    error(er);
                }
            });
        }
    }

    @Override
    public Bundle createBundle() {
        return consumer.createBundle();
    }

    @Override
    public void send(List<Bundle> bundles) {
        try {
            consumer.send(bundles);
        } catch (Exception e) {
            DataChannelError de = DataChannelError.promote(e);
            error(de);
            throw de;
        }
    }

    void error(DataChannelError ex) {
        if (error == null) {
            error = ex;

            meshSourceAggregator.meshQueryMaster.handleError(query);
            // Cancel the query through the query tracker
            meshSourceAggregator.meshQueryMaster.getQueryTracker().cancelRunning(query.uuid(), error.getMessage());

            // in case canceling through the track 'somehow' fails
            cancel(ex.getMessage());
            done();
        }
    }

    void done() {
        if (done.compareAndSet(false, true)) {
            if (error != null) {
                consumer.sourceError(error);
            } else {
                // start query op sendComplete in-thread
                consumer.sendComplete();
            }
        }
    }

    double getMeanRuntime() {
        double sum = 0;
        for (Long runtime : meshSourceAggregator.aggregateHandle.runtimes) {
            sum += runtime;
        }
        if (!meshSourceAggregator.aggregateHandle.runtimes.isEmpty()) {
            sum /= meshSourceAggregator.aggregateHandle.runtimes.size();
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
        int n = 0;
        double stdDevsAway = 0;

        for (Long runtime : runtimes) {
            x += runtime;
            x2 += runtime * runtime;
        }
        n = runtimes.size();

        if (n > 0) {
            double mean = x / n;

            // This is an approximation. With large n we can ignore the -1 in the n-1 in the denominator of the
            // variance. However, to avoid division by 0 we ignore it
            double variance = (x2 - 2 * x * mean + n * mean * mean) / n;
            stdDevsAway = mean + multipleStdDevs * Math.sqrt(variance);
        }

        return stdDevsAway;
    }


    public void handleStragglers() {
        for (QueryTaskSource taskSource : taskSources) {
            if (taskSource.oneHasResponded() || (taskSource.options.length == 0)) {
                continue;
            }
            QueryTaskSourceOption option = selectArbitraryInactiveOption(taskSource);
            if (option != null) {
                option.activate(meshSourceAggregator.meshy, meshSourceAggregator.queryOptions);
                AggregateConfig.totalStragglerCheckerRequests.inc();
                if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                    Query.emitTrace("Straggler detected for " + query.uuid() + " sending duplicate query to host: "
                                    + option.queryReference.getHostUUID());
                }
            }
        }
    }

    private static QueryTaskSourceOption selectArbitraryInactiveOption(QueryTaskSource taskSource) {
        for (QueryTaskSourceOption option : taskSource.options) {
            if (!option.isActive()) {
                return option;
            }
        }
        return null;
    }
}

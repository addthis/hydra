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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.source.QueryConsumer;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.query.util.QueryData;

class AggregateHandle implements QueryHandle, DataChannelOutput {

    MeshSourceAggregator meshSourceAggregator;
    //handles MUST by synchronized on
    final List<QueryTaskSource> handles;
    final QueryConsumer consumer;
    final AtomicInteger counter;
    final AtomicBoolean done = new AtomicBoolean(false);
    final Query query;
    volatile DataChannelError error;
    final AtomicBoolean errored = new AtomicBoolean(false);
    final AtomicBoolean canceled = new AtomicBoolean(false);
    final Set<Integer> completed;
    final Set<Integer> started;
    final List<Long> runtimes;
    final int _totalTasks;
    final Map<Integer, Set<QueryData>> sourcesByTaskID;

    AggregateHandle(MeshSourceAggregator meshSourceAggregator, final Query query, final DataChannelOutput consumer, Map<Integer, Set<QueryData>> sourcesByTaskID) {
        this.meshSourceAggregator = meshSourceAggregator;
        this.query = query;
        this.consumer = new QueryConsumer(consumer);
        this._totalTasks = meshSourceAggregator.totalTasks;
        this.counter = new AtomicInteger(0);
        this.handles = new ArrayList<>(meshSourceAggregator.totalTasks);
        this.completed = Collections.synchronizedSet(new HashSet<Integer>(meshSourceAggregator.totalTasks));
        this.started = Collections.synchronizedSet(new HashSet<Integer>(meshSourceAggregator.totalTasks));
        this.runtimes = new ArrayList<>(meshSourceAggregator.totalTasks);
        this.sourcesByTaskID = sourcesByTaskID;
    }

    public long getStartTime() {
        return meshSourceAggregator.startTime;
    }

    public void addHandle(QueryTaskSource handle) {
        synchronized (handles) {
            if (!canceled.get()) {
                this.handles.add(handle);
            } else {
                //does not add handle to handles to prevent a possible double cancel.
                // currently handles aren't used anywhere else so this is fine, otherwise
                // an extra flag or something can be easily added
                cancelHandle(handle, "Handle added after query was canceled");
            }
        }
    }

    @Override
    public void cancel(String message) //closes connections to workers and consumer
    {
        MeshSourceAggregator.log.warn("[aggregate.handle] cancel called.  canceled:" + canceled.get() + " erred:" + errored.get());
        // only cancel one time here
        if (canceled.compareAndSet(false, true)) {

            if (MeshSourceAggregator.log.isTraceEnabled()) {
                MeshSourceAggregator.log.trace("CANCEL called: Deleting metrics for query: " + query.uuid());
            }

            //Close connections to workers and call done if needed
            synchronized (handles) {
                for (QueryHandle handle : handles) {
                    cancelHandle(handle, message);
                }
            }
        }
    }

    void cancelHandle(QueryHandle handle, String message) {
        try {
            handle.cancel(message); //QuerySource usually
        } catch (Exception ex) {
            String msg = String.format("error cancelling query %s # %d message: %s",
                    query.uuid(), counter.incrementAndGet(), message);
            MeshSourceAggregator.log.warn(msg, ex);
        }
    }

    @Override
    public void send(Bundle bundle) throws DataChannelError {
        try {
            consumer.send(bundle);
        } catch (DataChannelError e) {
            throw error(e);
        } catch (Exception e) {
            throw error(DataChannelError.promote(e));
        }
    }

    public boolean markTaskStarted(int node) {
        return started.add(node);
    }

    public void markTaskCompleted(int node, QueryTaskSource querySource) {
        long completionTime = JitterClock.globalTime() - meshSourceAggregator.startTime;
        meshSourceAggregator.runtimeLock.lock();
        try {
            runtimes.add(completionTime);
            if (!completed.add(node) && querySource.lines > 0) {
                MeshSourceAggregator.log.warn(query.uuid() + " duplicate complete message for job taskId: " + node + " from sourceId: " + querySource.id + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
            }
        } finally {
            meshSourceAggregator.runtimeLock.unlock();
        }
        if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
            Query.emitTrace(query.uuid() + " sendComplete for job taskId:" + node + " and pos:" + counter.get() + " from sourceId: " + querySource.queryData.hostEntryInfo.getHostName() + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
        }
        sendComplete();
    }

    @Override
    public void sendComplete() {

        if (completed.size() == meshSourceAggregator.totalTasks) {
            done();
        }
    }

    @Override
    public void sourceError(DataChannelError er) {
        error(er);
    }

    @Override
    public Bundle createBundle() {
        return consumer.createBundle();
    }

    @Override
    public void send(List<Bundle> bundles) {
        try {
            consumer.send(bundles);
        } catch (DataChannelError e) {
            throw error(e);
        } catch (Exception e) {
            throw error(DataChannelError.promote(e));
        }
    }

    DataChannelError error(DataChannelError ex) {
        if (errored.compareAndSet(false, true)) {
            error = ex;
            MeshSourceAggregator.log.warn(query.uuid() + " error " + ex + " #" + counter.incrementAndGet(), ex);
            cancel(ex.getMessage()); //'usually' does nothing
            done();
        }
        return ex;
    }

    void done() {
        if (done.compareAndSet(false, true)) {

            if (MeshSourceAggregator.log.isTraceEnabled()) {
                MeshSourceAggregator.log.trace("Committing metrics for query: " + query.uuid() + " and deleting its metrics");
            }

            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace(query.uuid() + " done " + consumer + " #" + counter.incrementAndGet());
            }

            if (error != null) {
                consumer.sourceError(error);
            } else {
                // this can take a while so run in a thread
                MeshSourceAggregator.queryOpPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        consumer.sendComplete();
                    }
                });
            }
        }
    }

    public boolean isComplete(int node) {
        return completed.contains(node);
    }

    public boolean isStarted(int node) {
        return started.contains(node);
    }

    double getMeanRuntime() {
        meshSourceAggregator.runtimeLock.lock();
        double sum = 0;
        try {
            for (Long runtime : meshSourceAggregator.handle.runtimes) {
                sum += runtime;
            }
            if (meshSourceAggregator.handle.runtimes.size() > 0) {
                sum /= meshSourceAggregator.handle.runtimes.size();
            }
        } finally {
            meshSourceAggregator.runtimeLock.unlock();
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

        meshSourceAggregator.runtimeLock.lock();
        try {
            for (Long runtime : runtimes) {
                x += runtime;
                x2 += runtime * runtime;
            }
            n = runtimes.size();
        } finally {
            meshSourceAggregator.runtimeLock.unlock();
        }

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
        for (int node = 0; node < meshSourceAggregator.totalTasks; node++) {
            if (meshSourceAggregator.handle.isComplete(node)) {
                continue;
            }
            if (!meshSourceAggregator.handle.isStarted(node)) {
                synchronized (sourcesByTaskID) {
                    Set<QueryData> queryDataSet = sourcesByTaskID.get(node);
                    if (queryDataSet != null && queryDataSet.size() > 0) {
                        Iterator<QueryData> iterator = queryDataSet.iterator();
                        if (iterator.hasNext()) {
                            QueryData queryData = iterator.next();
                            MeshSourceAggregator.totalStragglerCheckerRequests.inc();
                            iterator.remove();
                            String id = meshSourceAggregator.requestQueryData(queryData, query);
                            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                                Query.emitTrace("Straggler detected for " + query.uuid() + " node " + node + " sending duplicate query to host: " + queryData.hostEntryInfo.getHostName() + " sourceId: " + id);
                            }
                        }
                    }
                }
            }
        }
    }
}

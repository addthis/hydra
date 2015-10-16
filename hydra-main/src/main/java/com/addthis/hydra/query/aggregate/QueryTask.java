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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.query.Query;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTask implements Runnable {

    static final Logger log = LoggerFactory.getLogger(QueryTask.class);

    private final MeshSourceAggregator sourceAggregator;

    private int pollFailures = 0;
    @Nullable final AutoField sourceField;
    private final Supplier<List<QueryTaskSource>> queryTaskSourceSupplier;

    public QueryTask(MeshSourceAggregator sourceAggregator) {
        this.sourceAggregator = sourceAggregator;
        this.sourceField = getSourceField(sourceAggregator.query);
        this.queryTaskSourceSupplier = createQueryTaskSourceSupplier(sourceAggregator);
    }

    @Override
    public void run() {
        try {
            if (sourceAggregator.queryPromise.isDone()) {
                return;
            }
            // channel is not currently writable, so return immediately and get rescheduled later
            if (!sourceAggregator.channelWritable) {
                sourceAggregator.needScheduling = true;
                return;
            }
            // NOTE: both provider and readBundles update sourceAggregator.completed
            List<QueryTaskSource> taskSources = queryTaskSourceSupplier.get();
            int bundlesProcessed = readBundles(taskSources, AggregateConfig.FRAME_READER_READS);
            if (bundlesProcessed > 0) {
                sourceAggregator.queryPromise.tryProgress(0, bundlesProcessed);
            }
            if (sourceAggregator.completed == sourceAggregator.totalTasks) {
                if (!sourceAggregator.queryPromise.trySuccess()) {
                    log.warn("Tried to complete queryPromise {} , but failed", sourceAggregator.queryPromise);
                }
            } else {
                if (bundlesProcessed > 0) {
                    pollFailures = 0;
                    sourceAggregator.executor.execute(this);
                } else {
                    pollFailures += 1;
                    rescheduleSelfWithBackoff();
                }
            }
        } catch (Throwable e) {
            if (!sourceAggregator.queryPromise.tryFailure(e)) {
                log.warn("Tried to fail queryPromise {} , but failed", sourceAggregator.queryPromise, e);
            }
        }
    }

    private int readBundles(List<QueryTaskSource> taskSources, int maxReads) throws Exception {
        int bundlesProcessed = 0;
        int complete = 0;
        boolean processedBundle = true;
        while (processedBundle && (bundlesProcessed < maxReads)) {
            complete = 0;
            processedBundle = false;
            for (QueryTaskSource taskSource : taskSources) {
                if (taskSource.complete()) {
                    complete++;
                    continue;
                }
                try {
                    Bundle nextBundle = taskSource.next();
                    if (nextBundle != null) {
                        maybeInjectSourceField(nextBundle, taskSource);
                        sourceAggregator.consumer.send(nextBundle);
                        processedBundle = true;
                        bundlesProcessed++;
                    } else if (!isActivated(taskSource) && !sourceAggregator.queryPromise.isDone()) {
                        log.debug("query task has no active options; attempting to lease one");
                        if (sourceAggregator.tryActivateSource(taskSource)) {
                            log.debug("task option leased and activated successfully");
                        }
                    }
                } catch (IOException io) {
                    if (taskSource.lines == 0) {
                        // This QuerySource does not have this file anymore. Signal to the caller that a retry may
                        // resolve the issue.
                        sourceAggregator.replaceQuerySource(taskSource);
                    } else {
                        // This query source has started sending lines. Need to fail the query.
                        throw io;
                    }
                }
            }
        }
        sourceAggregator.completed += complete;
        return bundlesProcessed;
    }

    private static boolean isActivated(QueryTaskSource taskSource) {
        return taskSource.oneHasResponded() || !taskSource.hasNoActiveSources();
    }

    private void maybeInjectSourceField(Bundle nextBundle, QueryTaskSource taskSource) {
        if (sourceField != null) {
            sourceField.setValue(nextBundle, ValueFactory.create(taskSource.getSelectedSource().queryReference.name));
        }
    }

    private void rescheduleSelfWithBackoff() {
        if (pollFailures <= 25) {
            sourceAggregator.executor.execute(this);
        } else if (pollFailures <= 150) {
            sourceAggregator.executor.schedule(this, 1, TimeUnit.MILLISECONDS);
        } else if (pollFailures <= 200) { // 125 ms - 625 ms
            sourceAggregator.executor.schedule(this, 10, TimeUnit.MILLISECONDS);
        } else if (pollFailures <= 250) { // 625 ms - 1875 ms
            sourceAggregator.executor.schedule(this, 25, TimeUnit.MILLISECONDS);
        } else { // > 1.875 seconds
            sourceAggregator.executor.schedule(this, 100, TimeUnit.MILLISECONDS);
        }
    }

    private Supplier<List<QueryTaskSource>> createQueryTaskSourceSupplier(MeshSourceAggregator sourceAggregator) {
        Query query = sourceAggregator.query;
        int totalTasks = sourceAggregator.totalTasks;
        int maxSimul = getMaxSimul(query.getParameter("maxSimul"), totalTasks);
        if (maxSimul == totalTasks) {
            return new DefaultQueryTaskSourceSupplier(sourceAggregator);
        } else {
            return new MaxSimulQueryTaskSourceSupplier(sourceAggregator, maxSimul);
        }
    }

    private int getMaxSimul(@Nullable String maxSimulQueryParam, int totalTasks) {
        Integer n = Ints.tryParse(Strings.nullToEmpty(maxSimulQueryParam));
        return (n != null && 0 < n && n <= totalTasks) ? n : totalTasks;
    }

    private static AutoField getSourceField(Query query) {
        String sourceFieldName = query.getParameter("injectSource");
        if (sourceFieldName != null) {
            try {
                return Configs.decodeObject(AutoField.class, sourceFieldName);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            return null;
        }
    }

    private static class DefaultQueryTaskSourceSupplier implements Supplier<List<QueryTaskSource>> {

        private final MeshSourceAggregator sourceAggregator;

        private DefaultQueryTaskSourceSupplier(MeshSourceAggregator sourceAggregator) {
            this.sourceAggregator = sourceAggregator;
        }

        @Override public List<QueryTaskSource> get() {
            List<QueryTaskSource> list = new ArrayList<>(sourceAggregator.totalTasks);
            int complete = 0;
            for (QueryTaskSource taskSource : sourceAggregator.taskSources) {
                if (taskSource.complete()) {
                    complete++;
                } else {
                    list.add(taskSource);
                }
            }
            sourceAggregator.completed = complete;
            return list;
        }
    }

    private static class MaxSimulQueryTaskSourceSupplier implements Supplier<List<QueryTaskSource>> {

        private final MeshSourceAggregator sourceAggregator;
        private final int maxSimul;

        private MaxSimulQueryTaskSourceSupplier(MeshSourceAggregator sourceAggregator, int maxSimul) {
            this.sourceAggregator = sourceAggregator;
            this.maxSimul = maxSimul;
        }

        @Override public List<QueryTaskSource> get() {
            List<QueryTaskSource> list = new ArrayList<>(maxSimul);
            List<QueryTaskSource> inactive = new ArrayList<>(sourceAggregator.totalTasks);
            int complete = 0;
            // separate active and inactive sources. add all active ones to the return list
            for (QueryTaskSource taskSource : sourceAggregator.taskSources) {
                if (taskSource.complete()) {
                    complete++;
                } else if (isActivated(taskSource)) {
                    list.add(taskSource);
                } else {
                    inactive.add(taskSource);
                }
            }
            sourceAggregator.completed = complete;
            // if not enough active sources, try to activate more add to the return list
            if (list.size() < maxSimul) {
                for (QueryTaskSource taskSource: inactive) {
                    if (sourceAggregator.tryActivateSource(taskSource)) {
                        list.add(taskSource);
                    }
                    if (list.size() == maxSimul) {
                        break;
                    }
                }
            }
            return list;
        }
    }
}

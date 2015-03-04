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

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.NoopField;
import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.query.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.bundle.value.ValueFactory.create;

public class QueryTask implements Runnable {

    static final Logger log = LoggerFactory.getLogger(QueryTask.class);

    private final MeshSourceAggregator sourceAggregator;

    private int pollFailures = 0;
    private final AutoField sourceField;

    public QueryTask(MeshSourceAggregator sourceAggregator) {
        this.sourceAggregator = sourceAggregator;
        this.sourceField = getSourceField(sourceAggregator.query);
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
            QueryTaskSource[] taskSources = sourceAggregator.taskSources;
            int bundlesProcessed = 0;
            int complete = 0;
            boolean processedBundle = true;
            while (processedBundle && (bundlesProcessed < AggregateConfig.FRAME_READER_READS)) {
                complete = 0;
                processedBundle = false;
                for (int i = 0; i < taskSources.length; i++) {
                    QueryTaskSource taskSource = taskSources[i];
                    if (taskSource.complete()) {
                        complete += 1;
                        continue;
                    }
                    try {
                        Bundle nextBundle = taskSource.next();
                        if (nextBundle != null) {
                            sourceField.setValue(nextBundle,
                                                 create(taskSource.getSelectedSource().queryReference.name));
                            sourceAggregator.consumer.send(nextBundle);
                            processedBundle = true;
                            bundlesProcessed += 1;
                        } else if (!taskSource.oneHasResponded()
                                   && taskSource.hasNoActiveSources()
                                   && !sourceAggregator.queryPromise.isDone()) {
                            log.debug("query task has no active options; attempting to lease one");
                            for (QueryTaskSourceOption option : taskSource.options) {
                                if (sourceAggregator.tryActivateSource(option)) {
                                    log.debug("task option leased and activated successfully");
                                    break;
                                }
                            }
                        }
                    } catch (IOException io) {
                        if (taskSource.lines == 0) {
                            // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                            sourceAggregator.replaceQuerySource(taskSource, taskSource.getSelectedSource());
                        } else {
                            // This query source has started sending lines. Need to fail the query.
                            throw io;
                        }
                    }
                }
            }
            if (bundlesProcessed > 0) {
                sourceAggregator.queryPromise.tryProgress(0, bundlesProcessed);
            }
            sourceAggregator.completed = complete;
            if (complete == taskSources.length) {
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

    private static AutoField getSourceField(Query query) {
        String sourceFieldName = query.getParameter("injectSource");
        if (sourceFieldName != null) {
            try {
                return Configs.decodeObject(AutoField.class, sourceFieldName);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            return new NoopField();
        }
    }
}

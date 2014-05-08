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

import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTask implements Runnable {

    static final Logger log = LoggerFactory.getLogger(QueryTask.class);

    private final MeshSourceAggregator sourceAggregator;

    public QueryTask(MeshSourceAggregator sourceAggregator) {
        this.sourceAggregator = sourceAggregator;
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
                            sourceAggregator.consumer.send(nextBundle);
                            processedBundle = true;
                            bundlesProcessed += 1;
                        }
                    } catch (IOException io) {
                        if (taskSource.lines == 0) {
                            // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                            sourceAggregator.replaceQuerySource(taskSource,
                                    taskSource.getSelectedSource(), i);
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
                    sourceAggregator.executor.execute(this);
                } else {
                    sourceAggregator.executor.schedule(this, 25, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Throwable e) {
            if (!sourceAggregator.queryPromise.tryFailure(e)) {
                log.warn("Tried to fail queryPromise {} , but failed", sourceAggregator.queryPromise, e);
            }
        }
    }
}

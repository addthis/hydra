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

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;

public class QueryTask implements Runnable {

    private final AggregateHandle aggregateHandle;

    public QueryTask(AggregateHandle aggregateHandle) {
        this.aggregateHandle = aggregateHandle;
    }

    @Override
    public void run() {
        try {
            if (aggregateHandle.canceled) {
                return;
            }
            if (aggregateHandle.done.get()) {
                MeshSourceAggregator.log.warn("Aggregate Handle was unexpectedly marked done");
                aggregateHandle.stopSources("aggregate handle already thinks it is done");
            }
            Query query = aggregateHandle.meshSourceAggregator.query;
            QueryTaskSource[] taskSources = aggregateHandle.taskSources;
            int bundlesProcessed = 0;
            int complete = 0;
            boolean processedBundle = true;
            while (processedBundle && (bundlesProcessed < AggregateConfig.FRAME_READER_READS)) {
                if (query != null && query.queryStatusObserver != null && query.queryStatusObserver.queryCompleted) {
                    aggregateHandle.stopSources("query completed early");
                    aggregateHandle.sendComplete();
                    return;
                }
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
                            aggregateHandle.send(nextBundle);
                            processedBundle = true;
                            bundlesProcessed += 1;
                        }
                    } catch (IOException io) {
                        if (taskSource.lines == 0) {
                            // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                            aggregateHandle.meshSourceAggregator.replaceQuerySource(taskSource,
                                    taskSource.getSelectedSource(), i);
                        } else {
                            // This query source has started sending lines. Need to fail the query.
                            throw io;
                        }
                    }
                }
            }
            aggregateHandle.completed = complete;
            if (complete == taskSources.length) {
                aggregateHandle.stopSources("source was already marked as complete");
                aggregateHandle.sendComplete();
            }  else {
                aggregateHandle.meshSourceAggregator.executor.execute(this);
            }
        } catch (Exception e) {
            aggregateHandle.sourceError(DataChannelError.promote(e));
        }
    }
}

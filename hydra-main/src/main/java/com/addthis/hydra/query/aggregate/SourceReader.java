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

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.meshy.service.file.FileReference;

class SourceReader implements Runnable {

    final MeshQueryMaster meshQueryMaster;

    SourceReader(MeshQueryMaster meshQueryMaster) {
        this.meshQueryMaster = meshQueryMaster;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        while (!MeshSourceAggregator.exiting.get()) {
            QueryTaskSource querySource = null;
            try {
                querySource = MeshSourceAggregator.readerQueue.poll(100, TimeUnit.MILLISECONDS);
                try {
                    processQuerySource(querySource);
                } catch (FileReferenceIOException ex) {
                    if (MeshSourceAggregator.log.isDebugEnabled()) {
                        MeshSourceAggregator.log.debug("Received IOException for task " + querySource.getKey() + "; attempting retry");
                    }
                    MeshSourceAggregator.totalRetryRequests.inc();
                    processQuerySource(replaceQuerySource(querySource));
                }
            } catch (Exception e) {
                // this is going to rethrow exception, so we need to replace this SourceReader in the pool
                MeshSourceAggregator.frameReaderPool.submit(new SourceReader(meshQueryMaster));
                handleQuerySourceError(querySource, e);
            }
        }
    }

    QueryTaskSource replaceQuerySource(QueryTaskSource querySource) throws IOException {
        // Invoked when a cached FileReference throws an IO Exception
        // Get a fresh FileReference and make a new QuerySource with that FileReference and the same parameters otherwise
        FileReference fileReference = meshQueryMaster.getReplacementFileReferenceForSingleTask(querySource.getJobId(), querySource.getTaskId(), querySource.getFileReference());
        return querySource.createCloneWithReplacementFileReference(fileReference);
    }

    void processQuerySource(QueryTaskSource querySource) throws DataChannelError, IOException, FileReferenceIOException {
        if (querySource != null) {
            // loops until the source is done, query is canceled
            // or source returns a null bundle which indicates its complete or just doesn't
            // have data at the moment
            while (!querySource.done && !querySource.consumer.canceled.get()) {
                boolean processedNext = false;
                try {
                    processedNext = processNextBundle(querySource);
                } catch (IOException io) {
                    if (querySource.lines == 0) {
                        // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                        throw new FileReferenceIOException();
                    }
                    else {
                        // This query source has started sending lines. Need to fail the query.
                        throw io;
                    }

                }

                if (!processedNext) {
                    // is the source exhausted, not canceled and not obsolete
                    if (querySource.done
                        && !querySource.obsolete
                        && !querySource.consumer.canceled.get()) {
                        // Save the time and lines in the hostEntryInfo
                        QueryData queryData = querySource.queryData;
                        queryData.hostEntryInfo.setLines(querySource.lines);
                        queryData.hostEntryInfo.setFinished();

                        // Mark this task as complete (and query if all done)
                        querySource.consumer.markTaskCompleted(queryData.taskId, querySource);
                        querySource.done = true;
                        querySource.close();

                        if (MeshSourceAggregator.log.isTraceEnabled()) {
                            MeshSourceAggregator.log.trace("Adding time & lines: QueryID: " + querySource.query.uuid() + " host:" + queryData.hostEntryInfo.getHostName());
                        }

                    } else if (!querySource.dataChannelReader.busy && querySource.done || querySource.obsolete || querySource.consumer.canceled.get()) {
                        if (MeshSourceAggregator.log.isTraceEnabled() || querySource.query.isTraced()) {
                            Query.emitTrace("ignoring response for query: " + querySource.query.uuid() + " from source: " + querySource.id + " for task: " + querySource.getTaskId() + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
                        }
                        QueryData queryData = querySource.queryData;
                        queryData.hostEntryInfo.setIgnored();
                    }
                    break;
                }
            }
            if (!querySource.consumer.done.get() && !querySource.consumer.errored.get() &&
                !querySource.done && !querySource.isEof() && !querySource.obsolete && !querySource.canceled) {
                // source still has more data but hasn't completed yet.
                MeshSourceAggregator.readerQueue.add(querySource);
            }
        }
    }

    void handleQuerySourceError(QueryTaskSource querySource, Exception error) {
        try {
            MeshSourceAggregator.log.warn("QueryError: " + error.getMessage() + " --- " + querySource.query.uuid() + ":" + querySource.getTaskId() + ":" + querySource.queryData.hostEntryInfo.getHostName() + " failed after receiving: " + querySource.lines + " lines");
            // invalidate file reference cache to prevent persistent errors from bogging everything down
            // TODO:  find more intelligent way to do this...
            meshQueryMaster.invalidateFileReferenceCache();

            // Cancel the query through the query tracker
            meshQueryMaster.getQueryTracker().cancelRunning(querySource.query.uuid(), error.getMessage());

            // Throw the exception so it can be seen in the logs
            if (error instanceof DataChannelError) {
                throw querySource.consumer.error((DataChannelError) error);
            } else {
                throw querySource.consumer.error(new DataChannelError(error));
            }
            // TODO: this flat doesn't work, not sure why so removing for now
            // check to see if we should resubmit query
            // resubmitQueryIfPossible(querySource, e);
        } catch (Exception e)  {
            MeshSourceAggregator.log.warn("", e);
            throw new RuntimeException(e);
        }
    }

    boolean processNextBundle(QueryTaskSource querySource) throws IOException, DataChannelError {
        Bundle nextBundle = querySource.next();
        if (querySource.canceled || querySource.obsolete) {
            return false;
        }

        if (querySource.query != null && querySource.query.queryStatusObserver != null && querySource.query.queryStatusObserver.queryCompleted) {
            if (MeshSourceAggregator.log.isDebugEnabled()) {
                MeshSourceAggregator.log.debug("Query complete flag is set. Finishing task: " + querySource.getTaskId());
            }
            querySource.done = true;
            return false;
        }

        if (nextBundle != null) {
            querySource.consumer.send(nextBundle);
            return !querySource.isEof();
        }
        return false;
    }
}

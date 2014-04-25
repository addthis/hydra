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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;

class QueryTaskSource implements QueryHandle {

    MeshSourceAggregator meshSourceAggregator;
    final QueryData queryData;
    final AggregateHandle consumer;
    final Query query;
    volatile boolean canceled = false;
    volatile boolean obsolete = false;
    volatile boolean done = false;
    final DataChannelCodec.ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
    final DataChannelCodec.FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();
    final SourceInputStream sourceInputStream;
    FramedDataChannelReader dataChannelReader;
    int lines;
    boolean foundBundle = false;
    AtomicBoolean started = new AtomicBoolean(false);
    String id = UUID.randomUUID().toString();

    QueryTaskSource(MeshSourceAggregator meshSourceAggregator, QueryData queryData, AggregateHandle consumer, Query query) {
        this.meshSourceAggregator = meshSourceAggregator;
        this.queryData = queryData;
        this.consumer = consumer;
        this.query = query;
        this.sourceInputStream = getInputStream();
        dataChannelReader = new FramedDataChannelReader(this.sourceInputStream, queryData.fileReference.name, classMap, fieldMap, MeshSourceAggregator.pollWaitTime);
        consumer.addHandle(this);
    }

    public SourceInputStream getInputStream() {
        FileReference fileReference = queryData.fileReference;
        StreamSource source = null;
        try {
            source = new StreamSource(queryData.channelMaster, fileReference.getHostUUID(), fileReference.getHostUUID(), fileReference.name, queryData.queryOptions, 0);
        } catch (IOException e) {
            MeshSourceAggregator.log.warn("Error getting query handle for fileReference: " + fileReference.getHostUUID() + "/" + fileReference.name, e);
            throw new QueryException(e);
        }
        return source.getInputStream();
    }

    Bundle next() throws IOException, DataChannelError {
        if (canceled || obsolete) {
            // canceled before we got a chance to run;
            return null;
        }

        if (started.compareAndSet(false, true)) {
            queryData.hostEntryInfo.start();
        }

        Bundle bundle;
        if ((bundle = dataChannelReader.read()) != null) {
            // check to see if this is the first response for this node id
            if (!markResponse()) {
                close();
                return null;
            }
            consumer.markTaskStarted(queryData.taskId);
            if (canceled) {
                consumer.sourceError(new DataChannelError("Query Canceled"));
                return null;
            }
            foundBundle = true;
            lines++;
            queryData.hostEntryInfo.setLines(lines);
        }
        if (isEof()) {
            done = true;
        }
        return bundle;
    }

    public FileReference getFileReference() {
        return queryData.fileReference;
    }

    public boolean isEof() {
        return dataChannelReader.eof.get();
    }

    boolean markResponse() {
        final String existingResponse = meshSourceAggregator.queryResponders.putIfAbsent(queryData.taskId, id);
        if (existingResponse != null && !existingResponse.equals(id)) {
            if (MeshSourceAggregator.log.isTraceEnabled() || query.isTraced()) {
                Query.emitTrace("FileReference: " + queryData.fileReference + " is not the first to respond for task ID: " + queryData.taskId + " ignoring results");
            }
            obsolete = true;
            queryData.hostEntryInfo.setIgnored();
            return false;
        } else {
            return true;
        }
    }

    public String getJobId() {
        return queryData.jobId;
    }

    @Override
    public void cancel(String message) {
        canceled = true;
        //Calls done which closes connection to consumer / entity what asked us a query
        consumer.sourceError(new DataChannelError(message));
        try {
            //Close worker connections
            if (dataChannelReader != null) {
                dataChannelReader.close();
            }
        } catch (Exception e) {
            MeshSourceAggregator.log.warn("Exception closing dataChannelReader: " + e.getMessage());
        }
    }

    public void close() {
        try {
            if (dataChannelReader != null) {
                dataChannelReader.close();
            }
        } catch (Exception e) {
            MeshSourceAggregator.log.warn("Exception closing dataChannelReader: " + e.getMessage());
        }
    }

    public Integer getTaskId() {
        return queryData.taskId;
    }

    public String getKey() {
        return getJobId() + "/" + getTaskId();
    }

    public String toString() {
        return queryData.jobId + "/" + queryData.taskId;
    }

    public QueryTaskSource createCloneWithReplacementFileReference(FileReference fileReference) {
        QueryData cloneQueryData = new QueryData(this.queryData.channelMaster, fileReference, this.queryData.queryOptions, this.getJobId(), this.getTaskId());
        return new QueryTaskSource(meshSourceAggregator, cloneQueryData, consumer, query);
    }
}

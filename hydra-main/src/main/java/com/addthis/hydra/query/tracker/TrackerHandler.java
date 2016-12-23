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

package com.addthis.hydra.query.tracker;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.query.aggregate.DetailedStatusTask;
import com.addthis.hydra.query.aggregate.MeshSourceAggregator;
import com.addthis.hydra.query.aggregate.TaskSourceInfo;
import com.addthis.hydra.query.web.DataChannelOutputToNettyBridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

public class TrackerHandler extends ChannelOutboundHandlerAdapter implements ChannelProgressiveFutureListener {

    private static final Logger log = LoggerFactory.getLogger(TrackerHandler.class);

    private final QueryTracker queryTracker;
    private final String[]     opsLog;
    private final MeshSourceAggregator aggregator;
    private final BundleField  typeField;
    private final BundleField  errorField;
    private final BundleField  pathField;
    private final BundleField  opsField;
    private final BundleField  sourcesField;
    private final BundleField  timeField;
    private final BundleField  runTimeField;
    private final BundleField  jobIdField;
    private final BundleField  jobAliasField;
    private final BundleField  queryIdField;
    private final BundleField  linesField;
    private final BundleField  sentLinesField;
    private final BundleField  senderField;

    // set when added to pipeline
    private DataChannelOutputToNettyBridge queryUser;
    private ChannelHandlerContext          ctx;
    ChannelProgressivePromise queryPromise;

    // set on query
    private Query            query;
    private QueryEntry       queryEntry;
    private QueryOpProcessor opProcessorConsumer;
    ChannelProgressivePromise opPromise;
    ChannelPromise            requestPromise;

    public TrackerHandler(QueryTracker queryTracker, String[] opsLog, MeshSourceAggregator aggregator) {
        this.queryTracker = queryTracker;
        this.opsLog = opsLog;
        this.aggregator = aggregator;
        BundleFormat eventFormat = queryTracker.eventLog.createBundle().getFormat();
        typeField = eventFormat.getField("type");
        errorField = eventFormat.getField("error");
        pathField = eventFormat.getField("path");
        opsField = eventFormat.getField("ops");
        sourcesField = eventFormat.getField("sources");
        timeField = eventFormat.getField("time");
        runTimeField = eventFormat.getField("run.time");
        jobIdField = eventFormat.getField("job.id");
        jobAliasField = eventFormat.getField("job.alias");
        queryIdField = eventFormat.getField("query.id");
        linesField = eventFormat.getField("lines");
        sentLinesField = eventFormat.getField("lines.sent");
        senderField = eventFormat.getField("sender");
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.queryPromise = ctx.newProgressivePromise();
        this.opPromise = ctx.newProgressivePromise();
        this.ctx = ctx;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof Query) {
            writeQuery(ctx, (Query) msg, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    protected void writeQuery(final ChannelHandlerContext ctx, Query msg, ChannelPromise promise)
            throws Exception {
        this.requestPromise = promise;
        this.queryUser = new DataChannelOutputToNettyBridge(ctx, promise);
        this.query = msg;
        query.queryPromise = queryPromise;
        // create a processor chain based in query ops terminating the query user
        this.opProcessorConsumer = query.newProcessor(queryUser, opPromise);
        queryEntry = new QueryEntry(query, opsLog, this, aggregator);

        // Check if the uuid is repeated, then make a new one
        if (queryTracker.running.putIfAbsent(query.uuid(), queryEntry) != null) {
            throw new QueryException("Query uuid somehow already in use : " + query.uuid());
        }

        log.debug("Executing.... {} {}", query.uuid(), queryEntry);

        ctx.pipeline().remove(this);

        opPromise.addListener(this);
        queryPromise.addListener(this);
        requestPromise.addListener(this);
        ctx.write(opProcessorConsumer, queryPromise);
    }

    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
        if (future == queryPromise) {
            queryEntry.preOpLines.addAndGet((int) total);
        } else {
            queryEntry.postOpLines.addAndGet((int) total);
        }
    }

    @Override
    public void operationComplete(ChannelProgressiveFuture future) throws Exception {
        if (future == queryPromise) {
            // get a snapshot of query sources after aggregation ends
            Promise<TaskSourceInfo[]> promise = new DefaultPromise<>(ctx.executor());
            submitDetailedStatusTask(new DetailedStatusTask(promise));
            queryEntry.lastSourceInfo = promise.getNow();
            if (queryEntry.queryState == QueryState.AGGREGATING) {
                queryEntry.queryState = QueryState.OPS;
            }
            return;
        } else if (future == opPromise) {
            // tell aggregator about potential early termination from the op promise
            if (future.isSuccess()) {
                queryPromise.trySuccess();
            } else {
                queryPromise.tryFailure(future.cause());
            }
            return;
        }
        // else the entire request is over; either from an error the last http write completing

        // tell the op processor about potential early termination (which may in turn tell aggre.)
        if (future.isSuccess()) {
            opPromise.trySuccess();
            queryEntry.queryState = QueryState.COMPLETE;
        } else {
            opPromise.tryFailure(future.cause());
            if (future.isCancelled()) {
                queryEntry.queryState = QueryState.CANCELLED;
            } else if (future.cause() instanceof TimeoutException){
                queryEntry.queryState = QueryState.TIMEOUT;
            } else {
                queryEntry.queryState = QueryState.ERROR;
            }
        }
        opProcessorConsumer.close();
        QueryEntry runE = queryTracker.running.remove(query.uuid());
        if (runE == null) {
            log.warn("failed to remove running for {}", query.uuid());
        }

        QueryEntryInfo entryInfo = queryEntry.getStat();
        TaskSourceInfo[] taskSourceInfos = entryInfo.tasks;
        if (taskSourceInfos == null) {
            log.warn("Failed to get detailed status for completed query {}; defaulting to brief",
                     query.uuid());
        } else {
            int exactLines = 0;
            for (TaskSourceInfo taskSourceInfo : taskSourceInfos) {
                exactLines += taskSourceInfo.lines;
            }
            entryInfo.lines = exactLines;
            entryInfo.tasks = taskSourceInfos;
        }

        try {
            Bundle event = queryTracker.eventLog.createBundle();
            event.setValue(pathField, ValueFactory.create(entryInfo.paths[0]));
            event.setValue(opsField, ValueFactory.create(Arrays.toString(entryInfo.ops)));
            event.setValue(sourcesField, ValueFactory.create(entryInfo.sources));
            event.setValue(timeField, ValueFactory.create(System.currentTimeMillis()));
            event.setValue(runTimeField, ValueFactory.create(entryInfo.runTime));
            event.setValue(jobIdField, ValueFactory.create(entryInfo.job));
            event.setValue(jobAliasField, ValueFactory.create(entryInfo.alias));
            event.setValue(queryIdField, ValueFactory.create(entryInfo.uuid));
            event.setValue(linesField, ValueFactory.create(entryInfo.lines));
            event.setValue(sentLinesField, ValueFactory.create(entryInfo.sentLines));
            event.setValue(senderField, ValueFactory.create(entryInfo.sender));
            if (!future.isSuccess()) {
                Throwable queryFailure = future.cause();
                event.setValue(typeField, ValueFactory.create("error"));
                event.setValue(errorField, ValueFactory.create(queryFailure.getMessage()));
                queryTracker.queryErrors.inc();
            } else {
                event.setValue(typeField, ValueFactory.create("complete"));
            }
            queryTracker.recentlyCompleted.put(query.uuid(), entryInfo);
            queryTracker.queryMeter.update(entryInfo.runTime, TimeUnit.MILLISECONDS);
            queryTracker.eventLog.send(event);
        } catch (Exception e) {
            log.error("Error while doing record keeping for a query.", e);
        }
    }

    public void submitDetailedStatusTask(DetailedStatusTask task) {
        ctx.write(task);
    }

}

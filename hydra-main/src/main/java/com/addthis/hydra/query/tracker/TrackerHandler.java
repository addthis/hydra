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

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.query.aggregate.DetailedStatusTask;
import com.addthis.hydra.query.aggregate.TaskSourceInfo;
import com.addthis.hydra.query.web.DataChannelOutputToNettyBridge;
import com.addthis.hydra.util.StringMapHelper;

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
    private final String[] opsLog;

    // set when added to pipeline
    private DataChannelOutputToNettyBridge queryUser;
    private ChannelHandlerContext ctx;
    ChannelProgressivePromise queryPromise;

    // set on query
    private Query query;
    private QueryEntry queryEntry;
    private QueryOpProcessor opProcessorConsumer;
    ChannelProgressivePromise opPromise;
    ChannelPromise requestPromise;

    public TrackerHandler(QueryTracker queryTracker, String[] opsLog) {
        this.queryTracker = queryTracker;
        this.opsLog = opsLog;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.queryPromise = ctx.newProgressivePromise();
        this.opPromise = ctx.newProgressivePromise();
        this.ctx = ctx;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Query) {
            writeQuery(ctx, (Query) msg, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    protected void writeQuery(final ChannelHandlerContext ctx, Query msg, ChannelPromise promise) throws Exception {
        this.requestPromise = promise;
        this.queryUser = new DataChannelOutputToNettyBridge(ctx, promise);
        this.query = msg;
        query.queryPromise = queryPromise;
        // create a processor chain based in query ops terminating the query user
        this.opProcessorConsumer = query.newProcessor(queryUser, opPromise);
        queryEntry = new QueryEntry(query, opsLog, this);

        // Check if the uuid is repeated, then make a new one
        if (queryTracker.running.putIfAbsent(query.uuid(), queryEntry) != null) {
            throw new QueryException("Query uuid somehow already in use : " + query.uuid());
        }

        log.debug("Executing.... {} {}", query.uuid(), queryEntry.queryDetails);

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
            StringMapHelper queryLine = new StringMapHelper()
                    .put("query.path", entryInfo.paths[0])
                    .put("query.ops", Arrays.toString(entryInfo.ops))
                    .put("sources", entryInfo.sources)
                    .put("time", System.currentTimeMillis())
                    .put("time.run", entryInfo.runTime)
                    .put("job.id", entryInfo.job)
                    .put("job.alias", entryInfo.alias)
                    .put("query.id", entryInfo.uuid)
                    .put("lines", entryInfo.lines)
                    .put("sentLines", entryInfo.sentLines)
                    .put("sender", entryInfo.sender);
            if (!future.isSuccess()) {
                Throwable queryFailure = future.cause();
                queryLine.put("type", "query.error")
                         .put("error", queryFailure.getMessage());
                queryTracker.queryErrors.inc();
            } else {
                queryLine.put("type", "query.done");
            }
            queryTracker.recentlyCompleted.put(query.uuid(), entryInfo);
            queryTracker.log(queryLine);
            queryTracker.queryMeter.update(entryInfo.runTime, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error while doing record keeping for a query.", e);
        }
    }

    public void submitDetailedStatusTask(DetailedStatusTask task) {
        ctx.write(task);
    }

}

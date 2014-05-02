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

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.query.aggregate.DetailedStatusTask;
import com.addthis.hydra.query.web.DataChannelOutputToNettyBridge;
import com.addthis.hydra.util.StringMapHelper;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.SimpleChannelInboundHandler;

public class TrackerHandler extends SimpleChannelInboundHandler<Query> implements ChannelProgressiveFutureListener {

    private final QueryTracker queryTracker;
    private final String[] opsLog;

    // set when added to pipeline
    private DataChannelOutputToNettyBridge queryUser;
    private ChannelProgressivePromise queryPromise;
    private ChannelHandlerContext ctx;

    // set on query
    private Query query;
    private QueryEntry queryEntry;
    private QueryOpProcessor opProcessorConsumer;
    private ChannelProgressivePromise opPromise;

    public TrackerHandler(QueryTracker queryTracker, String[] opsLog) {
        this.queryTracker = queryTracker;
        this.opsLog = opsLog;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.queryPromise = ctx.newProgressivePromise();
        this.opPromise = ctx.newProgressivePromise();
        this.queryUser = new DataChannelOutputToNettyBridge(ctx);
        this.ctx = ctx;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Query msg) throws Exception {
        this.query = msg;
        query.queryPromise = queryPromise;
        // create a processor chain based in query ops terminating the query user
        this.opProcessorConsumer = query.newProcessor(queryUser, opPromise);
        queryTracker.calls.inc();
        queryEntry = new QueryEntry(query, opsLog, queryPromise, this);

        QueryTracker.log.debug("Executing.... {} {}", query.uuid(), queryEntry.queryDetails);

        // Check if the uuid is repeated, then make a new one
        if (queryTracker.running.putIfAbsent(query.uuid(), queryEntry) != null) {
            String old = query.uuid();
            query.useNextUUID();
            QueryTracker.log.warn("Query uuid was already in use in running. Going to try assigning a new one. old:{} new:{}",
                    old, query.uuid());
            if (queryTracker.running.putIfAbsent(query.uuid(), queryEntry) != null) {
                throw new QueryException("Query uuid was STILL somehow already in use : " + query.uuid());
            }
        }

        // TODO: metrics about hosts / tasks
//            for (QueryData querydata : queryDataCollection) {
//                entry.addHostEntryInfo(querydata.hostEntryInfo);
//            }

        queryPromise.addListener(this);
        ctx.write(opProcessorConsumer, queryPromise);
//        QueryTracker.log.warn("Exception thrown while running query: {}", query.uuid(), ex);
    }

    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
        if (progress == 0) {
            queryEntry.preOpLines.addAndGet((int) total);
        } else {
            queryEntry.postOpLines.addAndGet((int) total);
        }
    }

    @Override
    public void operationComplete(ChannelProgressiveFuture future) throws Exception {
        queryEntry.runTime = queryEntry.getRunTime();

        QueryEntry runE = queryTracker.running.remove(query.uuid());
        if (runE == null) {
            QueryTracker.log.warn("failed to remove running for {}", query.uuid());
        }

//        queryTracker.queryGate.release();

        try {
            StringMapHelper queryLine = new StringMapHelper()
                    .put("query.path", query.getPaths()[0])
                    .put("query.ops", Arrays.toString(opsLog))
                    .put("sources", query.getParameter("sources"))
                    .put("time", System.currentTimeMillis())
                    .put("time.run", queryEntry.runTime)
                    .put("job.id", query.getJob())
                    .put("job.alias", query.getParameter("track.alias"))
                    .put("query.id", query.uuid())
                    .put("lines", queryEntry.preOpLines.get())
                    .put("sender", query.getParameter("sender"));
            if (!future.isSuccess()) {
                Throwable queryFailure = future.cause();
                queryLine.put("type", "query.error")
                         .put("error", queryFailure.getMessage());
                queryTracker.queryErrors.inc();
                opPromise.tryFailure(queryFailure);
            } else {
                queryLine.put("type", "query.done");
                queryTracker.recentlyCompleted.put(query.uuid(), queryEntry.getStat());
            }
            queryTracker.log(queryLine);
        } catch (Exception e) {
            QueryTracker.log.warn("Error while doing record keeping for a query.", e);
        }
    }

    public void submitDetailedStatusTask (DetailedStatusTask task) {
        ctx.write(task);
    }
}

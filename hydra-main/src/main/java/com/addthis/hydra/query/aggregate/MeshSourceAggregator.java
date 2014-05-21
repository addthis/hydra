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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.meshy.ChannelMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;

public class MeshSourceAggregator extends ChannelDuplexHandler implements ChannelFutureListener {

    static final Logger log = LoggerFactory.getLogger(MeshSourceAggregator.class);

    final QueryTaskSource[] taskSources;
    final int totalTasks;
    final long startTime;
    final ChannelMaster meshy;
    final Map<String, String> queryOptions;
    final MeshQueryMaster meshQueryMaster;
    final Query query;

    // set when added to a pipeline
    EventExecutor executor;

    // set when write (query) is called
    ChannelProgressivePromise queryPromise;
    DataChannelOutput consumer;
    Runnable queryTask;

    // optionally set near the end of write
    ScheduledFuture<?> stragglerTaskFuture;

    boolean channelWritable;
    boolean needScheduling;

    // set periodically by query task
    int completed;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                AggregateConfig.exiting.set(true);
            }
        });
    }

    public MeshSourceAggregator(QueryTaskSource[] taskSources, ChannelMaster meshy,
            MeshQueryMaster meshQueryMaster, Query query) {
        this.taskSources = taskSources;
        this.meshy = meshy;
        this.meshQueryMaster = meshQueryMaster;
        this.query = query;
        totalTasks = taskSources.length;
        this.startTime = JitterClock.globalTime();

        queryOptions = new HashMap<>();
        queryOptions.put("query", CodecJSON.encodeString(query));
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DataChannelOutput) {
            queryPromise = (ChannelProgressivePromise) promise;
            consumer = (DataChannelOutput) msg;
            AggregateConfig.totalQueries.inc();
            queryPromise.addListener(this);
            meshQueryMaster.allocators().allocateQueryTasks(query, taskSources, meshy, queryOptions);
            queryTask = new QueryTask(this);
            if (ctx.channel().isWritable()) {
                channelWritable = true;
                executor.execute(queryTask);
            }
            maybeScheduleStragglerChecks();
        } else if (msg instanceof DetailedStatusTask) {
            DetailedStatusTask task = (DetailedStatusTask) msg;
            task.run(this);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        executor = ctx.executor();
    }

    void maybeScheduleStragglerChecks() {
        if (AggregateConfig.enableStragglerCheck) {
            Runnable stragglerCheckTask = new StragglerCheckTask(this);
            int checkPeriod = AggregateConfig.stragglerCheckPeriod;
            // just have it reschedule itself since that's what recurring tasks are for, right?
            stragglerTaskFuture = executor.schedule(stragglerCheckTask, checkPeriod, TimeUnit.MILLISECONDS);
        }
    }

    void stopSources(String message) {
        for (QueryTaskSource taskSource : taskSources) {
            taskSource.cancelAllActiveOptions(message);
        }
    }

    boolean tryActivateSource(QueryTaskSourceOption option) {
        return option.tryActivate(meshy, queryOptions);
    }

    void replaceQuerySource(QueryTaskSource taskSource, QueryTaskSourceOption option,
            int taskId) throws Exception {
        taskSource.reset();
        // Invoked when a cached FileReference throws an IO Exception
        // Get a fresh FileReference and make a new QuerySource with that FileReference
        //      and the same parameters otherwise
        QueryTaskSourceOption newOption = meshQueryMaster.getReplacementQueryTaskOption(query.getJob(),
                taskId, option.queryReference);
        for (int i = 0; i < taskSource.options.length; i++) {
            if (taskSource.options[i] == option) {
                taskSource.options[i] = newOption;
            }
        }
        newOption.tryActivate(meshy, queryOptions);
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        future.channel().pipeline().remove(this);

        // make sure this auto-recurring task doesn't go on forever
        if (stragglerTaskFuture != null) {
            stragglerTaskFuture.cancel(true);
        }
        if (!future.isSuccess()) {
            stopSources(future.cause().getMessage());
            meshQueryMaster.handleError(query);
            consumer.sourceError(DataChannelError.promote((Exception) future.cause()));
        } else {
            stopSources("query is complete");
            consumer.sendComplete();
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        channelWritable = ctx.channel().isWritable();
        if (channelWritable && needScheduling) {
            executor.execute(queryTask);
        }
    }
}

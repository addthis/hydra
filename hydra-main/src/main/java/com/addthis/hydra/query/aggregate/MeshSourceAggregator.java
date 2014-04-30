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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.service.file.FileReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;

public class MeshSourceAggregator extends ChannelOutboundHandlerAdapter {

    static final Logger log = LoggerFactory.getLogger(MeshSourceAggregator.class);

    final QueryTaskSource[] sourcesByTaskID;
    final int totalTasks;
    final long startTime;
    final ChannelMaster meshy;
    final Map<String, String> queryOptions;
    final MeshQueryMaster meshQueryMaster;
    final Query query;

    EventExecutor executor;

    // set when write is called
    AggregateHandle aggregateHandle;
    ChannelPromise queryPromise;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                AggregateConfig.exiting.set(true);
            }
        });
    }

    public MeshSourceAggregator(QueryTaskSource[] sourcesByTaskID, ChannelMaster meshy,
            Map<String, String> queryOptions, MeshQueryMaster meshQueryMaster, Query query) {
        this.sourcesByTaskID = sourcesByTaskID;
        this.meshy = meshy;
        this.queryOptions = queryOptions;
        this.meshQueryMaster = meshQueryMaster;
        this.query = query;
        totalTasks = sourcesByTaskID.length;
        this.startTime = JitterClock.globalTime();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DataChannelOutput) {
            queryPromise = promise;
            query((DataChannelOutput) msg);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        executor = ctx.executor();
    }

    public void query(DataChannelOutput consumer) throws Exception {
        AggregateConfig.totalQueries.inc();
        aggregateHandle = new AggregateHandle(this, query, consumer, sourcesByTaskID);
        TaskAllocator.allocateQueryTasks(query, sourcesByTaskID, meshy, queryOptions);
        Runnable queryProcessingTask = new QueryTask(aggregateHandle);
        executor.execute(queryProcessingTask);
        maybeScheduleStragglerChecks();
    }

    void maybeScheduleStragglerChecks() {
        if (AggregateConfig.enableStragglerCheck) {
            Runnable stragglerCheckTask = new StragglerCheckTask(aggregateHandle);
            int checkPeriod = AggregateConfig.stragglerCheckPeriod;
            // just have it reschedule itself so that we don't have to do as much book keeping later
            executor.schedule(stragglerCheckTask, checkPeriod, TimeUnit.MILLISECONDS);
        }
    }

    void replaceQuerySource(QueryTaskSource taskSource, QueryTaskSourceOption option,
            int taskId) throws IOException {
        taskSource.reset();
        // Invoked when a cached FileReference throws an IO Exception
        // Get a fresh FileReference and make a new QuerySource with that FileReference and the same parameters otherwise
        FileReference fileReference = meshQueryMaster.getReplacementFileReferenceForSingleTask(query.getJob(),
                taskId, option.queryReference);
        QueryTaskSourceOption newOption = new QueryTaskSourceOption(fileReference);
        for (int i = 0; i < taskSource.options.length; i++) {
            if (taskSource.options[i] == option) {
                taskSource.options[i] = newOption;
            }
        }
        newOption.activate(meshy, queryOptions);
    }
}

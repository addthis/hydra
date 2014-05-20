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

package com.addthis.hydra.query.loadbalance;

import com.addthis.hydra.query.web.HttpUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.EventExecutorGroup;

public class NextQueryTask implements Runnable, ChannelFutureListener {

    private static final Logger log = LoggerFactory.getLogger(NextQueryTask.class);

    public final QueryQueue queryQueue;
    public final EventExecutorGroup executor;

    public NextQueryTask(QueryQueue queryQueue, EventExecutorGroup executor) {
        this.queryQueue = queryQueue;
        this.executor = executor;
    }

    @Override
    public void run() {
        QueryRequest request;
        try {
            request = queryQueue.takeQuery();
        } catch (InterruptedException ignored) {
            log.info("Frame reader thread interrupted -- halting query processing");
            return;
        }
        try {
            ChannelFuture queryFuture = HttpQueryCallHandler.handleQuery(
                    request.querySource, request.kv, request.request, request.ctx, executor);
            queryFuture.addListener(this);
        } catch (Exception e) {
            log.warn("Exception caught while serving http query endpoint", e);
            if (request.ctx.channel().isActive()) {
                HttpUtils.sendError(request.ctx, new HttpResponseStatus(500, e.getMessage()));
            }
        }
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        // schedule next query poll
        executor.execute(this);
    }
}

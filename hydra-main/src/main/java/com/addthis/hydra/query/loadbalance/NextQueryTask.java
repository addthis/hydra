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

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import com.addthis.hydra.query.web.HttpUtils;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.EventExecutor;

public class NextQueryTask implements Runnable, ChannelFutureListener {

    private static final Logger log = LoggerFactory.getLogger(NextQueryTask.class);

    public final QueryQueue queryQueue;
    public final EventExecutor executor;

    public NextQueryTask(QueryQueue queryQueue, EventExecutor executor) {
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
            log.warn("Exception caught before mesh query master added to pipeline", e);
            if (request.ctx.channel().isActive()) {
                HttpUtils.sendError(request.ctx, new HttpResponseStatus(500, e.getMessage()));
            }
        }
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        log.trace("complete called");
        if (!future.isSuccess()) {
            log.warn("Exception caught while serving http query endpoint", future.cause());
            ChannelPipeline pipeline = future.channel().pipeline();
            log.trace("pipeline before pruning {}", pipeline);
            while(!"encoder".equals(pipeline.lastContext().name())) {
                pipeline.removeLast();
            }
            log.trace("pipeline after pruning {}", pipeline);
            if (future.channel().isActive()) {
                sendDetailedError(pipeline.lastContext(), future.cause());
            }
        }
        // schedule next query poll
        executor.execute(this);
        log.trace("rescheduled");
    }

    public static void sendDetailedError(ChannelHandlerContext ctx, Throwable cause) {
        if (cause == null) {
            cause = new RuntimeException("query failed for unknown reasons");
        }
        String reasonPhrase = cause.getMessage();
        String detailPhrase = Throwables.getStackTraceAsString(cause);
        HttpResponseStatus responseStatus;
        try {
            responseStatus = new HttpResponseStatus(500, reasonPhrase);
        } catch (NullPointerException | IllegalArgumentException ignored) {
            reasonPhrase = cause.getClass().getSimpleName();
            responseStatus = new HttpResponseStatus(500, reasonPhrase);
        }
        if (cause instanceof CancellationException) {
            detailPhrase = "Query was Cancelled by a User";
        } else if (cause instanceof TimeoutException) {
            detailPhrase = "Query timed out";
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus,
                Unpooled.copiedBuffer(detailPhrase + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        log.trace("issuing error of {}", responseStatus);

        // Close the connection as soon as the error message is sent.
        ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}

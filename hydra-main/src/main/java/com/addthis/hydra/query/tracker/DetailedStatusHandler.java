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

import java.nio.CharBuffer;

import com.addthis.codec.CodecJSON;
import com.addthis.hydra.query.aggregate.DetailedStatusTask;
import com.addthis.hydra.query.aggregate.TaskSourceInfo;
import com.addthis.hydra.query.web.HttpUtils;
import com.addthis.maljson.JSONObject;

import org.apache.commons.io.output.StringBuilderWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class DetailedStatusHandler implements FutureListener<TaskSourceInfo[]> {

    private final StringBuilderWriter writer;
    private final HttpResponse response;
    private final ChannelHandlerContext ctx;
    private final FullHttpRequest request;
    private final QueryEntry queryEntry;

    public DetailedStatusHandler(StringBuilderWriter writer, HttpResponse response, ChannelHandlerContext ctx,
            FullHttpRequest request, QueryEntry queryEntry) {
        this.writer = writer;
        this.response = response;
        this.ctx = ctx;
        this.request = request;
        this.queryEntry = queryEntry;
    }

    public void handle() {
        if (queryEntry != null) {
            if (queryEntry.lastSourceInfo == null) {
                Promise<TaskSourceInfo[]> promise = new DefaultPromise<>(ctx.executor());
                promise.addListener(this);
                DetailedStatusTask statusTask = new DetailedStatusTask(promise);
                queryEntry.trackerHandler.submitDetailedStatusTask(statusTask) ;
            } else {
                onSuccess(queryEntry.getStat());
            }
        }
        else {
            onFailure(new RuntimeException("query entry unexpectedly null"));
        }
    }

    private void onSuccess(QueryEntryInfo queryEntryInfo) {
        try {
            JSONObject entryJSON = CodecJSON.encodeJSON(queryEntryInfo);
            writer.write(entryJSON.toString());
            ByteBuf textResponse = ByteBufUtil.encodeString(ctx.alloc(),
                                                            CharBuffer.wrap(writer.getBuilder()), CharsetUtil.UTF_8);
            HttpContent content = new DefaultHttpContent(textResponse);
            response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, textResponse.readableBytes());
            if (HttpHeaders.isKeepAlive(request)) {
                response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }
            ctx.write(response);
            ctx.write(content);
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            onFailure(t);
        }
    }

    private void onFailure(Throwable cause) {
        if (ctx.channel().isActive()) {
            HttpUtils.sendError(ctx, new HttpResponseStatus(500, cause.getMessage()));
        }
    }

    @Override
    public void operationComplete(Future<TaskSourceInfo[]> future) throws Exception {
        if (future.isSuccess()) {
            try {
                TaskSourceInfo[] taskSourceInfos = future.get();
                QueryEntryInfo queryEntryInfo = queryEntry.getStat();
                long exactLines = 0;
                for (TaskSourceInfo taskSourceInfo : taskSourceInfos) {
                    exactLines += taskSourceInfo.lines;
                }
                queryEntryInfo.lines = exactLines;
                queryEntryInfo.tasks = taskSourceInfos;
                onSuccess(queryEntryInfo);
            } catch (Throwable t) {
                onFailure(t);
            }
        } else {
            onFailure(future.cause());
        }
    }
}

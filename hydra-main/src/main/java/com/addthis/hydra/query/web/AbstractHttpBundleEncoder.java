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

package com.addthis.hydra.query.web;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.query.web.HttpUtils.sendError;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * parent of all streaming response classes
 */
abstract class AbstractHttpBundleEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(AbstractHttpBundleEncoder.class);

    protected final HttpResponse responseStart = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    protected final long startTime;

    protected boolean writeStarted = false;

    AbstractHttpBundleEncoder() {
        HttpHeaders.setTransferEncodingChunked(responseStart);
        startTime = System.currentTimeMillis();
    }

    protected void writeStart(ChannelHandlerContext ctx) {
        ctx.write(responseStart);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Bundle) {
            send(ctx, (Bundle) msg);
        } else if (msg instanceof Exception) {
            sourceError(ctx, (Exception) msg);
        } else if (msg == DataChannelOutputToNettyBridge.SEND_COMPLETE) {
            sendComplete(ctx);
        } else {
            super.write(ctx, msg, promise); // forward write to next handler
        }
    }

    private void maybeWriteStart(ChannelHandlerContext ctx) {
        if (!writeStarted) {
            writeStart(ctx);
            writeStarted = true;
        }
    }

    public void batchSend(ChannelHandlerContext ctx, List<Bundle> rows) {
        maybeWriteStart(ctx);
    }

    public void send(ChannelHandlerContext ctx, Bundle row) {
        maybeWriteStart(ctx);
    }

    public void sendComplete(ChannelHandlerContext ctx) {
        maybeWriteStart(ctx);
        HttpQueryCallHandler.queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    public static void sourceError(ChannelHandlerContext ctx, Exception ex) {
        try {
            sendError(ctx, new HttpResponseStatus(500, ex.getMessage()));
            log.error("", ex);
        } catch (Exception e) {
            log.warn("Exception sending error", e);
        }
    }
}
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

import java.nio.CharBuffer;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.core.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

/**
 * parent of all streaming response classes
 */
abstract class AbstractBufferingHttpBundleEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(AbstractBufferingHttpBundleEncoder.class);

    private static final int DEFAULT_INITIAL_BUFFER_SIZE = Parameter.intValue("qmaster.http.buffer.initial", 100);
    private static final int DEFAULT_BATCH_BUFFER_SIZE = Parameter.intValue("qmaster.http.buffer.batch", 100000);

    protected final HttpResponse responseStart = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    private final StringBuilder sendBuffer;
    private final int batchBufferSize;

    private boolean writeStarted = false;


    AbstractBufferingHttpBundleEncoder(int initialBufferSize, int batchBufferSize) {
        this.batchBufferSize = batchBufferSize;
        HttpHeaders.setTransferEncodingChunked(responseStart);
        sendBuffer = new StringBuilder(initialBufferSize);
    }

    AbstractBufferingHttpBundleEncoder() {
        this(DEFAULT_INITIAL_BUFFER_SIZE, DEFAULT_BATCH_BUFFER_SIZE);
    }

    private static ByteBuf encodeString(ByteBufAllocator alloc, CharSequence msg) {
        return ByteBufUtil.encodeString(alloc, CharBuffer.wrap(msg), CharsetUtil.UTF_8);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Bundle) {
            send(ctx, (Bundle) msg);
        } else if (msg == DataChannelOutputToNettyBridge.SEND_COMPLETE) {
            sendComplete(ctx);
            ctx.pipeline().remove(this);
            // no need to make the frame reader wait on the async flush to finish
            promise.trySuccess();
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        } else {
            super.write(ctx, msg, promise); // forward write to next handler
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (sendBuffer.length() > 0) {
            flushStringBuilder(ctx);
        } else {
            ctx.flush();
        }
    }

    private boolean maybeWriteStart(ChannelHandlerContext ctx, Bundle row) {
        if (!writeStarted) {
            ctx.write(responseStart);
            appendResponseStartToString(sendBuffer);
            if (row != null) {
                appendInitialBundleToString(row, sendBuffer);
            }
            writeStarted = true;
            return true;
        }
        return false;
    }

    public abstract void appendBundleToString(Bundle row, StringBuilder sendBuffer);

    /**
     * Called before any bundles are written.
     */
    protected void appendResponseStartToString(StringBuilder sendBuffer) {
        // override in subclasses if desired
    }

    /**
     * The bundle firstRow is passed in and should be written in this method.
     * It is provided due to the frequent case of the first row requiring special
     * logic. If (first/ not first) is the only context needed to encode a bundle,
     * then subsequent bundles can be encoded concurrently.
     */
    protected void appendInitialBundleToString(Bundle firstRow, StringBuilder sendBuffer) {
        appendBundleToString(firstRow, sendBuffer);
    }

    /**
     * Called after all bundles are written.
     */
    protected void appendResponseEndToString(StringBuilder sendBuffer) {
        // override in subclasses if desired
    }

    public void send(ChannelHandlerContext ctx, Bundle row) {
        if (!maybeWriteStart(ctx, row)) {
            appendBundleToString(row, sendBuffer);
        }
        if (sendBuffer.length() >= batchBufferSize) {
            flushStringBuilder(ctx);
        }
    }

    protected void flushStringBuilder(ChannelHandlerContext ctx) {
        if (sendBuffer.length() > 0) {
            ByteBuf msg = encodeString(ctx.alloc(), sendBuffer);
            sendBuffer.setLength(0);
            ctx.writeAndFlush(new DefaultHttpContent(msg), ctx.voidPromise());
        }
    }

    public void sendComplete(ChannelHandlerContext ctx) {
        maybeWriteStart(ctx, null);
        appendResponseEndToString(sendBuffer);
        flushStringBuilder(ctx);
    }
}
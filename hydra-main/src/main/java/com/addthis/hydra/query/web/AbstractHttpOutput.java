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

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.query.web.HttpUtils.sendError;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

/**
 * parent of all streaming response classes
 */
abstract class AbstractHttpOutput implements DataChannelOutput {

    private static final Logger log = LoggerFactory.getLogger(AbstractHttpOutput.class);
    private static final StringEncoder stringer = new StringEncoder(CharsetUtil.UTF_8);

    protected final ListBundleFormat format = new ListBundleFormat();
    protected final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    protected final long startTime;
    protected final ChannelHandlerContext ctx;

    protected boolean writeStarted = false;

    AbstractHttpOutput(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        HttpHeaders.setTransferEncodingChunked(response);
        ctx.pipeline().addBefore("query", "stringer", stringer);
        startTime = System.currentTimeMillis();
    }

    protected void writeStart() {
        ctx.write(response);
    }

    @Override
    public synchronized void send(Bundle bundle) {
        if (!writeStarted) {
            writeStart();
            writeStarted = true;
        }
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override
    public void sourceError(DataChannelError ex) {
        try {
            sendError(ctx, new HttpResponseStatus(500, ex.getMessage()));
            log.error("", ex);
        } catch (Exception e) {
            log.warn("Exception sending error", e);
        }
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }

    @Override
    public void sendComplete() { // TODO: keep alive logic
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        lastContentFuture.addListener(ChannelFutureListener.CLOSE);
    }
}
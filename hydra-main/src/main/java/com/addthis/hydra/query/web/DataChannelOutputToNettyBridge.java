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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.LastHttpContent;

/**
 * parent of all streaming response classes
 */
public class DataChannelOutputToNettyBridge implements DataChannelOutput {

    private static final Logger log = LoggerFactory.getLogger(DataChannelOutputToNettyBridge.class);

    protected final ListBundleFormat format = new ListBundleFormat();
    protected final ChannelHandlerContext ctx;
    static final Object SEND_COMPLETE = new Object();

    public DataChannelOutputToNettyBridge(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void send(Bundle bundle) {
        log.info("Writing bundle to pipeline {}", bundle);
        ctx.write(bundle);
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                ctx.write(bundle);
            }
        }
    }

    @Override
    public void sourceError(DataChannelError ex) {
        log.info("Writing exception to pipeline", ex);
        ctx.write(ex);
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }

    @Override
    public void sendComplete() { // TODO: keep alive logic
        log.info("Writing sendComplete to pipeline");
        ctx.write(SEND_COMPLETE);
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        lastContentFuture.addListener(ChannelFutureListener.CLOSE);
    }
}
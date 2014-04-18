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

import io.netty.channel.DefaultMessageSizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class QueryServerInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger log = LoggerFactory.getLogger(QueryServerInitializer.class);

    private final HttpQueryHandler httpQueryHandler;

    public QueryServerInitializer(HttpQueryHandler httpQueryHandler) {
        this.httpQueryHandler = httpQueryHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        log.trace("New socket connection {}", ch);
        ch.config().setMessageSizeEstimator(new DefaultMessageSizeEstimator(200));
        ch.config().setWriteBufferHighWaterMark(100000000);
        ch.config().setWriteBufferLowWaterMark(50000000);
        pipeline.addLast("decoder", new HttpRequestDecoder(163840,163840,163840));
        pipeline.addLast("aggregator", new HttpObjectAggregator(163840));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        // compression is neat, but a little buggy
//        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("query", httpQueryHandler);

    }
}

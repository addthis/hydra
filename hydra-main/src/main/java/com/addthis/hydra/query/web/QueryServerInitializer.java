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

import com.addthis.basis.util.Parameter;
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

    private static final int maxInitialLineLength = Parameter.intValue("queryServerInitializer.maxInitialLineLength", 327680);
    private static final int maxHeaderSize = Parameter.intValue("queryServerInitializer.maxHeaderSize", 327680);
    private static final int maxChunkSize = Parameter.intValue("queryServerInitializer.maxChunkSize", 327680);
    private static final int maxContentLength = Parameter.intValue("queryServerInitializer.maxChunkSize", 327680);

    private final HttpQueryHandler httpQueryHandler;

    public QueryServerInitializer(HttpQueryHandler httpQueryHandler) {
        this.httpQueryHandler = httpQueryHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        log.trace("New socket connection {}", ch);
        pipeline.addLast("decoder", new HttpRequestDecoder(maxInitialLineLength,maxHeaderSize,maxChunkSize));
        pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        // compression is neat, but a little buggy
//        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("query", httpQueryHandler);

    }
}

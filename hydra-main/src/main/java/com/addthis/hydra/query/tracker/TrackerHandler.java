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

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class TrackerHandler extends SimpleChannelInboundHandler<Query> {


    private final QueryOpProcessor opProcessorConsumer;
    private final String[] opsLog;

    private Query query;

    public TrackerHandler(QueryOpProcessor opProcessorConsumer, String[] opsLog) {
        this.opProcessorConsumer = opProcessorConsumer;
        this.opsLog = opsLog;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Query msg) throws Exception {
        query = msg;
    }
}

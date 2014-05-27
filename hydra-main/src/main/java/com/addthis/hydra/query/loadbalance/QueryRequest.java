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

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.query.MeshQueryMaster;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

public class QueryRequest {

    final MeshQueryMaster querySource;
    final KVPairs kv;
    final HttpRequest request;
    final ChannelHandlerContext ctx;
    final long queueStartTime = System.currentTimeMillis();

    public QueryRequest(MeshQueryMaster querySource, KVPairs kv, HttpRequest request,
            ChannelHandlerContext ctx) {
        this.querySource = querySource;
        this.kv = kv;
        this.request = request;
        this.ctx = ctx;
    }

}

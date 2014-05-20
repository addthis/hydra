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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.query.MeshQueryMaster;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

public class QueryQueue {

    static final int DEFAULT_QUEUE_MAX = Parameter.intValue("query.queue.max", 1000);
    static final Timer queueTimer = Metrics.newTimer(QueryQueue.class, "queueTimer", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    static final Counter queueCount = Metrics.newCounter(QueryQueue.class, "queueCount");

    public final BlockingQueue<QueryRequest> blockingQueue;

    public QueryQueue() {
        this(DEFAULT_QUEUE_MAX);
    }

    public QueryQueue(int maxQueueSize) {
        this.blockingQueue = new LinkedBlockingQueue<>(maxQueueSize);
    }

    public void queueQuery(MeshQueryMaster querySource, KVPairs kv, HttpRequest request,
            ChannelHandlerContext ctx) throws Exception {
        QueryRequest queryRequest = new QueryRequest(querySource, kv, request, ctx);
        // throws exception when queue is full - callers should handle appropriately
        blockingQueue.add(queryRequest);
        queueCount.inc();
    }

    public QueryRequest takeQuery() throws InterruptedException {
        QueryRequest request = blockingQueue.take();
        queueTimer.update(System.currentTimeMillis() - request.queueStartTime, TimeUnit.MILLISECONDS);
        queueCount.dec();
        return request;
    }
}

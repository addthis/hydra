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

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.aggregate.AggregateConfig;
import com.addthis.hydra.query.loadbalance.NextQueryTask;
import com.addthis.hydra.query.loadbalance.QueryQueue;
import com.addthis.hydra.query.tracker.QueryTracker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;

public class QueryServer {

    private static final Logger log = LoggerFactory.getLogger(QueryServer.class);
    private static final int DEFAULT_WEB_PORT = Parameter.intValue("qmaster.web.port", 2222);
    private static final int queryPort = Parameter.intValue("qmaster.query. port", 2601);
    private static final boolean accessLogEnabled = Parameter.boolValue("qmaster.log.accessLogging", true);
    private static final String accessLogDir = Parameter.value("qmaster.log.accessLogDir", "log/qmaccess");
    static final Counter rawQueryCalls = Metrics.newCounter(MeshQueryMaster.class, "rawQueryCalls");
    static final JsonFactory factory = new JsonFactory(new ObjectMapper());

    private final HttpQueryHandler httpQueryHandler;
    private final QueryServerInitializer queryServerInitializer;
    private final QueryQueue queryQueue;
    private final int webPort;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup executorGroup;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && (args[0].equals("--help") || args[0].equals("-h"))) {
            System.out.println("usage: qmaster");
        }
        QueryServer qm = new QueryServer();
        qm.run();
    }

    public QueryServer() throws Exception {
        this(DEFAULT_WEB_PORT);
    }

    public QueryServer(int webPort) throws Exception {
        this.webPort = webPort;

        QueryTracker queryTracker = new QueryTracker();
        MeshQueryMaster meshQueryMaster = new MeshQueryMaster(queryTracker);
        queryQueue = new QueryQueue();
        httpQueryHandler = new HttpQueryHandler(this, queryTracker, meshQueryMaster, queryQueue);
        queryServerInitializer = new QueryServerInitializer(httpQueryHandler);

        log.info("[init] query port={}, web port={}", queryPort, webPort);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    public void run() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        executorGroup = new DefaultEventExecutorGroup(AggregateConfig.FRAME_READER_THREADS,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("frame-reader-%d").build());
        for (EventExecutor executor : executorGroup) {
            executor.execute(new NextQueryTask(queryQueue, executor));
        }
        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_BACKLOG, 1024);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.childOption(ChannelOption.MESSAGE_SIZE_ESTIMATOR, new DefaultMessageSizeEstimator(200));
        b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 100000000);
        b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 50000000);
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(queryServerInitializer);
        b.bind(webPort).sync();
    }

    protected void shutdown() {
        try {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully().sync();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully().sync();
            }
            if (executorGroup != null) {
                // no sync because there is apparently no easy way to interrupt the take() calls?
                executorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static HandlerCollection wrapWithLogging(Handler seedHandler) {
        HandlerCollection handlers = new HandlerCollection();
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Files.initDirectory(accessLogDir);
        NCSARequestLog requestLog = new NCSARequestLog(accessLogDir + "/jetty-yyyy_mm_dd.request.log");
        requestLog.setPreferProxiedForAddress(true);
        requestLog.setRetainDays(35);
        requestLog.setAppend(true);
        requestLog.setExtended(true);
        requestLog.setLogLatency(true);
        // TODO: America/NY?
        requestLog.setLogTimeZone("EST");
        requestLogHandler.setRequestLog(requestLog);

        handlers.addHandler(seedHandler);
        handlers.addHandler(requestLogHandler);
        return handlers;
    }
}

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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.channel.QueryChannelServer;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.QueryTracker;
import com.addthis.hydra.util.MetricsServletMaker;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServer extends AbstractHandler {

    private static final Logger log = LoggerFactory.getLogger(QueryServer.class);
    private static final int webPort = Parameter.intValue("qmaster.web.port", 2222);
    private static final int queryPort = Parameter.intValue("qmaster.query. port", 2601);
    private static final String logDir = Parameter.value("qmaster.log.dir", "log");
    private static final boolean eventLogCompress = Parameter.boolValue("qmaster.eventlog.compress", true);
    private static final int logMaxAge = Parameter.intValue("qmaster.log.maxAge", 60 * 60 * 1000);
    private static final int logMaxSize = Parameter.intValue("qmaster.log.maxSize", 100 * 1024 * 1024);
    private static final boolean accessLogEnabled = Parameter.boolValue("qmaster.log.accessLogging", true);
    private static final String accessLogDir = Parameter.value("qmaster.log.accessLogDir", "log/qmaccess");
    private static final int headerBufferSize = Parameter.intValue("qmaster.headerBufferSize", 163840);

    static final String webDir = Parameter.value("qmaster.web.dir", "web");
    static final Counter rawQueryCalls = Metrics.newCounter(MeshQueryMaster.class, "rawQueryCalls");
    static final JsonFactory factory = new JsonFactory(new ObjectMapper());

    /**
     * server that listens for query requests using java protocol
     */
    private final QueryChannelServer queryServer;

    /**
     * server listens for query requests using HTML protoc
     */
    private final Server htmlQueryServer;

    /**
     * thread pool for jetty
     */
    final ThreadPool queuedThreadPool = new QueuedThreadPool(500);

    private final QueryHandler queryHandler;

    public static void main(String args[]) throws Exception {
        if (args.length > 0 && (args[0].equals("--help") || args[0].equals("-h"))) {
            System.out.println("usage: qmaster");
        }
        QueryServer qm = new QueryServer();
        qm.start();
    }

    public QueryServer() throws Exception {

        Query.setTraceLog(new RollingLog(new File(logDir, "events-trace"), "queryTrace", eventLogCompress, logMaxSize, logMaxAge));

        RollingLog eventLog = new RollingLog(new File(logDir, "events-query"), "query", eventLogCompress, logMaxSize, logMaxAge);

        QueryTracker queryTracker = new QueryTracker(eventLog);
        MeshQueryMaster meshQueryMaster = new MeshQueryMaster(queryTracker);
        ServletHandler metricsHandler = MetricsServletMaker.makeHandler();
        queryHandler = new QueryHandler(this, queryTracker, meshQueryMaster, metricsHandler);

        queryServer = new QueryChannelServer(queryPort, meshQueryMaster);
        queryServer.start();
        htmlQueryServer = startHtmlQueryServer();

        log.info("[init] query port=" + queryPort + ", web port=" + htmlQueryServer.getConnectors()[0].getPort());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    protected void shutdown() {
        try {
            htmlQueryServer.stop();
            queryServer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        queryHandler.handle(target, baseRequest, httpServletRequest, httpServletResponse);
    }

    private Server startHtmlQueryServer() throws Exception {
        Server htmlQueryServer = new Server(webPort);
        if (accessLogEnabled) {
            htmlQueryServer.setHandler(wrapWithLogging(this));
        } else {
            htmlQueryServer.setHandler(this);
        }
        Connector connector0 = htmlQueryServer.getConnectors()[0];
        connector0.setMaxIdleTime(600000);
        connector0.setRequestBufferSize(headerBufferSize);
        connector0.setRequestHeaderSize(headerBufferSize);
        htmlQueryServer.setAttribute("headerBufferSize", headerBufferSize);
        htmlQueryServer.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize", -1);

        htmlQueryServer.setThreadPool(queuedThreadPool);
        htmlQueryServer.start();
        return htmlQueryServer;
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

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
package com.addthis.hydra.query;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.common.util.CloseTask;
import com.addthis.hydra.data.query.source.MeshQuerySource;
import com.addthis.hydra.data.query.source.SearchRunner;
import com.addthis.hydra.query.web.QueryServer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.reporting.MetricsServlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sort of wrapper class for mesh query workers; similar to {@link QueryServer} but this one only starts up a
 * jetty server to display metrics and then starts up meshy as if meshy was the class whose main method was invoked.
 * <p/>
 * For query worker's actual query processing logic, {@link MeshQuerySource}.
 */
public class MeshQueryWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MeshQueryWorker.class);

    /**
     * The port on which the worker reports metrics. The default is fine unless at some point we want multiple
     * mqworkers on one machine. If that happens, use codec to give them different ports.
     */
    private final int webPort;

    /** server listens for query requests using HTML protoc */
    private final transient Server htmlQueryServer;

    /** thread pool for jetty */
    private final transient ThreadPool queuedThreadPool = new QueuedThreadPool(20);

    public static void main(String[] args) throws Exception {
        final MeshQueryWorker queryWorker = Configs.newDefault(MeshQueryWorker.class);
        Runtime.getRuntime().addShutdownHook(new Thread(new CloseTask(queryWorker), "Query Worker Shutdown Hook"));

        // execute meshy main method and rely on system properties to register query file system
        com.addthis.meshy.Main.main(args);
    }

    @JsonCreator
    private MeshQueryWorker(@JsonProperty(value = "webPort", required = true) int webPort) throws Exception {
        // start jetty for metrics servlet
        this.webPort = webPort;
        this.htmlQueryServer = startHtmlQueryServer();
        log.info("[init]  web port={}", webPort);
    }

    @Override public void close() throws Exception {
        htmlQueryServer.stop();
        SearchRunner.shutdownSearchPool();
    }

    /**
     * Starts the jetty server, makes the metrics servlet, and registers it with jetty
     *
     * @return Jetty Server
     * @throws Exception
     */
    private Server startHtmlQueryServer() throws Exception {
        Server newHtmlServer = new Server(webPort);

        //Using a servlet as a handler requires a lot of boilerplate
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        newHtmlServer.setHandler(context);

        //Actually create the servlet (from yammer metrics)
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

        Connector connector0 = newHtmlServer.getConnectors()[0];
        connector0.setMaxIdleTime(600000);

        newHtmlServer.setThreadPool(queuedThreadPool);
        newHtmlServer.start();
        return newHtmlServer;
    }
}

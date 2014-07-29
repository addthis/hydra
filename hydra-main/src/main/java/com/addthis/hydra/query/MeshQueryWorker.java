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

import java.io.Closeable;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.codec.config.Configs;

import com.google.common.base.Throwables;

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
 * A sort of wrapper class for mesh query workers; similar to MeshQueryMaster but this one only starts up
 * a jetty server to spit out metrics for nest and then starts up meshy the same way meshy used to be called
 * from main.
 * <p/>
 * FOR ACTUAL QUERY WORKER LOGIC: see MeshQuerySource
 * <p/>
 * Has a shutdown hook which calls stop on the jetty server -- presumably helpful; copied from MQM
 */
public class MeshQueryWorker implements SuperCodable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(MeshQueryWorker.class);

    /**
     * The port on which the worker reports metrics. The default is fine unless at some point
     * we have multiple mqworkers on one machine for some reason. If that happens, use the system
     * property to give them different ports.
     */
    @FieldConfig(required = true)
    private int webPort;

    /** server listens for query requests using HTML protoc */
    private transient Server htmlQueryServer;

    /** thread pool for jetty */
    private final transient ThreadPool queuedThreadPool = new QueuedThreadPool(20);

    public static void main(String[] args) throws Exception {
        final MeshQueryWorker qs = Configs.newDefault(MeshQueryWorker.class);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                qs.close();
            }
        });
        //run meshy as per current standard
        com.addthis.meshy.Main.main(args);
    }

    @Override public void postDecode() {
        try {
            //start jetty for metrics servlet
            htmlQueryServer = startHtmlQueryServer();
            log.info("[init]  web port={}", webPort);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    @Override public void preEncode() {}

    @Override public void close() {
        try {
            htmlQueryServer.stop();
        } catch (Exception e) {
            log.error("mystery error while shutting down query worker's http server", e);
        }
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

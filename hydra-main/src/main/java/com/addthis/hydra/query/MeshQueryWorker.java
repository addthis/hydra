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

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import java.net.InetSocketAddress;

import com.addthis.basis.jvm.Shutdown;
import com.addthis.basis.util.LessStrings;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.query.source.MeshQuerySource;
import com.addthis.hydra.data.query.source.SearchRunner;
import com.addthis.hydra.query.web.QueryServer;
import com.addthis.hydra.util.PrometheusServletCreator;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.MeshyServerGroup;

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

    private final MeshyServer meshyServer;

    /** server listens for query requests using HTML protoc */
    private final transient Server htmlQueryServer;

    public static void main(String[] args) throws Exception {
        Shutdown.createWithShutdownHook(() -> {
            try {
                return Configs.newDefault(MeshQueryWorker.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, MeshQueryWorker::close);
    }

    @JsonCreator
    private MeshQueryWorker(@JsonProperty(value = "webPort", required = true) int webPort,
                            @JsonProperty(value = "bindAddress", required = true) String bindAddress,
                            @JsonProperty(value = "root", required = true) File root,
                            @JsonProperty(value = "peers", required = true) String peers) throws Exception {
        // start jetty for metrics servlet
        this.webPort = webPort;
        this.htmlQueryServer = startHtmlQueryServer();

        String[] portInfo = LessStrings.splitArray(bindAddress, ":");
        int portNum = Integer.parseInt(portInfo[0]);
        @Nullable String[] netIf;
        if (portInfo.length > 1) {
            netIf = new String[portInfo.length - 1];
            System.arraycopy(portInfo, 1, netIf, 0, netIf.length);
        } else {
            netIf = null;
        }
        this.meshyServer = new MeshyServer(portNum, root, netIf, new MeshyServerGroup());
        for (String peer : LessStrings.splitArray(peers, ",")) {
            String[] hostPort = LessStrings.splitArray(peer, ":");
            int port;
            if (hostPort.length > 1) {
                port = Integer.parseInt(hostPort[1]);
            } else {
                port = meshyServer.getLocalAddress().getPort();
            }
            meshyServer.connectPeer(new InetSocketAddress(hostPort[0], port));
        }
        log.info("[init]  web port={}, mesh server={}", webPort, meshyServer);
    }

    @Override public void close() {
        SearchRunner.shutdownSearchPool();
        meshyServer.close();
        try {
            htmlQueryServer.stop();
        } catch (Throwable e) {
            log.error("Error closing http server for mqworker", e);
        }
    }

    /** Starts the jetty server, makes the metrics servlet, and registers it with jetty */
    private Server startHtmlQueryServer() throws Exception {
        Server newHtmlServer = new Server(webPort);

        //Using a servlet as a handler requires a lot of boilerplate
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        PrometheusServletCreator.create(newHtmlServer, context);
        newHtmlServer.setHandler(context);

        //Actually create the servlet (from yammer metrics)
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

        Connector connector0 = newHtmlServer.getConnectors()[0];
        connector0.setMaxIdleTime(600000);

        newHtmlServer.start();
        return newHtmlServer;
    }
}

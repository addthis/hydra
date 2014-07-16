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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.lang.reflect.Field;

import java.net.InetAddress;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;

import com.addthis.hydra.data.query.Query;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.reporting.MetricsServlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
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
public class MeshQueryWorker {

    private static final  Logger log = LoggerFactory.getLogger(MeshQueryWorker.class);
    private static final boolean eventLogCompress = Parameter.boolValue("qworker.eventlog.compress", true);
    private static final int logMaxAge = Parameter.intValue("qworker.log.maxAge", 60 * 60 * 1000);
    private static final int logMaxSize = Parameter.intValue("qworker.log.maxSize", 100 * 1024 * 1024);
    private static final String logDir = Parameter.value("qworker.log.dir", "log");
    private static final String propFileName = Parameter.value("qworker.propfile", "mqworker.prop");

    /**
     * The port on which the worker reports metrics. The default is fine unless at some point
     * we have multiple mqworkers on one machine for some reason. If that happens, use the system
     * property to give them different ports.
     */
    private static final int webPort = Parameter.intValue("qworker.web.port", 2223);

    /**
     * server listens for query requests using HTML protoc
     */
    private final Server htmlQueryServer;

    /**
     * thread pool for jetty
     */
    final ThreadPool queuedThreadPool = new QueuedThreadPool(20);

    /**
     * metrics handler servlet
     */
    private final ServletHandler metricsHandler;

    /**
     * For JSON writing
     */
    private static final JsonFactory factory = new JsonFactory(new ObjectMapper());

    public static void main(String[] args) throws Exception {
        //start jetty and metrics servlet
        MeshQueryWorker qs = new MeshQueryWorker();
        //run meshy as per current standard
        com.addthis.meshy.Main.main(args);
    }

    /**
     * This constructor was copied from MeshQueryMaster and then stripped down for parts.
     * It just starts up jetty and gives it a metrics servlet from the yammer metrics package.
     * <p/>
     * It also creates the shutdown hook to stop jetty.
     *
     * @throws Exception
     */
    public MeshQueryWorker() throws Exception {
        Query.setTraceLog(new RollingLog(new File(logDir, "events-worker"), "queryTraceWorker", eventLogCompress, logMaxSize, logMaxAge));

        this.metricsHandler = new ServletHandler();
        htmlQueryServer = startHtmlQueryServer();

        log.info("[init]  web port=" + webPort);

        writePropertiesFile();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    protected void shutdown() {
        try {
            htmlQueryServer.stop();
            File propFile = new File(logDir, propFileName);
            if (propFile.exists()) {
                propFile.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts the jetty server, makes the metrics servlet, and registers it with jetty
     *
     * @return Jetty Server
     * @throws Exception
     */
    private Server startHtmlQueryServer() throws Exception {
        Server htmlQueryServer = new Server(webPort);

        //Using a servlet as a handler requires a lot of boilerplate
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        htmlQueryServer.setHandler(context);

        //Actually create the servlet (from yammer metrics)
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

        //For viewing/modifying system arbitrary parameters on the fly -- instead of say, mbeans
//      context.addServlet(new ServletHolder(new ReflectionServlet()),"/reflect/*");

        Connector connector0 = htmlQueryServer.getConnectors()[0];
        connector0.setMaxIdleTime(600000);

        htmlQueryServer.setThreadPool(queuedThreadPool);
        htmlQueryServer.start();
        return htmlQueryServer;
    }

    /**
     * Primitive http api which exposes from reflection-based functionality.
     * <p/>
     * Does not currently support setting variables -- would need some kind of concurrency enforcement probably.
     */
    private static class ReflectionServlet extends HttpServlet {

        public ReflectionServlet() {

        }

        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_OK);
            final String path = request.getPathInfo();
            response.getWriter().println("pathInfo: " + path);
            final String[] args = path.split("/");
            for (String arg : args) {
                response.getWriter().println(arg);
            }
            if (args.length < 3) {
                //whatever man
            } else { //we cool
                try {
                    Class theClass = Class.forName(args[1]);
                    Field theField = theClass.getDeclaredField(args[2]);
                    theField.setAccessible(true);
                    response.getWriter().println("Value: " + theField.get(null));
                } catch (Exception e) {
                    response.getWriter().println(e.toString());
                }
            }
        }
    }

    private void writePropertiesFile() {
        try {
            File propFile = new File(logDir, propFileName);
            FileWriter writer = new FileWriter(propFile);
            final JsonGenerator json = factory.createJsonGenerator(writer);
            json.writeStartObject();
            json.writeNumberField("webPort", webPort);
            json.writeStringField("webHost", InetAddress.getLocalHost().getHostName());
            json.writeEndObject();
            json.close();
        } catch (Exception ex) {
            log.warn("Error creating mqworker properties file.");
            ex.printStackTrace();
        }
    }
}

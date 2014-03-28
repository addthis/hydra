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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;

import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.channel.QueryChannelServer;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.QueryServlet;
import com.addthis.hydra.query.QueryTracker;
import com.addthis.hydra.query.WebSocketManager;
import com.addthis.hydra.query.util.HostEntryInfo;
import com.addthis.hydra.util.MetricsServletMaker;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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
    private static final String webDir = Parameter.value("qmaster.web.dir", "web");
    private static final boolean eventLogCompress = Parameter.boolValue("qmaster.eventlog.compress", true);
    private static final int logMaxAge = Parameter.intValue("qmaster.log.maxAge", 60 * 60 * 1000);
    private static final int logMaxSize = Parameter.intValue("qmaster.log.maxSize", 100 * 1024 * 1024);
    private static final boolean accessLogEnabled = Parameter.boolValue("qmaster.log.accessLogging", true);
    private static final String accessLogDir = Parameter.value("qmaster.log.accessLogDir", "log/qmaccess");
    private static final int headerBufferSize = Parameter.intValue("qmaster.headerBufferSize", 163840);
    private static final Counter rawQueryCalls = Metrics.newCounter(MeshQueryMaster.class, "rawQueryCalls");

    private static final JsonFactory factory = new JsonFactory(new ObjectMapper());

    /**
     * server that listens for query requests using java protocol
     */
    private final QueryChannelServer queryServer;

    /**
     * server listens for query requests using HTML protoc
     */
    private final Server htmlQueryServer;

    /**
     * used for tracking metrics and other interesting things about queries
     * that we have run.  Provides insight into currently running queries
     * and gives ability to cancel a query before it completes.
     */
    private final QueryTracker tracker;

    /**
     * thread pool for jetty
     */
    final ThreadPool queuedThreadPool = new QueuedThreadPool(500);

    /**
     * metrics handler servlet
     */
    private final ServletHandler metricsHandler;

    /**
     * primary query source
     */
    private final MeshQueryMaster meshQueryMaster;

    public static void main(String args[]) throws Exception {
        if (args.length > 0 && (args[0].equals("--help") || args[0].equals("-h"))) {
            System.out.println("usage: qmaster");
        }
        QueryServer qm = new QueryServer();
        qm.start();
    }

    public QueryServer() throws Exception {
        this.metricsHandler = MetricsServletMaker.makeHandler();

        Query.setTraceLog(new RollingLog(new File(logDir, "events-trace"), "queryTrace", eventLogCompress, logMaxSize, logMaxAge));

        RollingLog eventLog = new RollingLog(new File(logDir, "events-query"), "query", eventLogCompress, logMaxSize, logMaxAge);

        tracker = new QueryTracker(eventLog);
        this.meshQueryMaster = new MeshQueryMaster(tracker);

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
        try {
            handler(target, baseRequest, httpServletRequest, httpServletResponse);
        } catch (Exception ex) {
            httpServletResponse.sendError(500, "Handle Error " + ex);
            ex.printStackTrace();
        }
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

    private PrintWriter startResponse(HttpServletResponse response, String cbf, String cba) throws Exception {
        PrintWriter writer = response.getWriter();
        if (cbf != null) {
            response.setContentType("application/javascript; charset=utf-8");
            writer.write(cbf + "(");
            if (cba != null) writer.write(cba + ",");
        } else {
            response.setContentType("application/json; charset=utf-8");
        }
        return writer;
    }

    private void endResponse(PrintWriter writer, String cbf) throws Exception {
        if (cbf != null) {
            writer.write(");");
        }
    }

    private void handler(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        response.setBufferSize(65535);
        KVPairs kv = new KVPairs();
        boolean handled = true;
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements(); ) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        String cbf = kv.getValue("cbfunc");
        String cba = kv.getValue("cbfunc-arg");
        boolean jsonp = cbf != null;
        if (target.equals("/")) {
            response.sendRedirect("/query/index.html");
        } else if (target.startsWith("/metrics")) {
            metricsHandler.handle(target, baseRequest, request, response);
        } else if (target.equals("/q/")) {
            response.sendRedirect("/query/call?" + kv.toString());
        } else if (target.equals("/query/call") || target.equals("/query/call/")) {
            // TODO jsonp enable
            rawQueryCalls.inc();
            QueryServlet.handleQuery(meshQueryMaster, kv, request, response);
        } else if (target.equals("/query/list")) {
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write("[\n");
            for (QueryTracker.QueryEntryInfo stat : tracker.getRunning()) {
                writer.write(CodecJSON.encodeString(stat).concat(",\n"));
            }
            writer.write("]");
            endResponse(writer, cbf);
        } else if (target.equals("/completed/list")) {
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write("[\n");
            for (QueryTracker.QueryEntryInfo stat : tracker.getCompleted()) {
                writer.write(CodecJSON.encodeString(stat).concat(",\n"));
            }
            writer.write("]");
            endResponse(writer, cbf);
        } else if (target.equals("/host/list")) {
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write("[\n");
            for (HostEntryInfo hostEntryInfo : tracker.getQueryHosts(kv.getValue("uuid"))) {
                writer.write("{'hostname':'" + hostEntryInfo.getHostName() + "','lines':'" + hostEntryInfo.getLines() + "','starttime':" + hostEntryInfo.getStarttime() + ", 'finished':'" + hostEntryInfo.getFinished() + "', 'endtime':" + hostEntryInfo.getEndtime() + ", 'runtime':" + hostEntryInfo.getRuntime() + "},");
            }
            writer.write("]");
            endResponse(writer, cbf);
        } else if (target.equals("/query/cancel")) {
            PrintWriter writer = startResponse(response, cbf, cba);
            int status = 200;
            if (tracker.cancelRunning(kv.getValue("uuid"))) {
                if (jsonp) writer.write("{canceled:true,message:'");
                writer.write("canceled " + kv.getValue("uuid"));
            } else {
                if (jsonp) writer.write("{canceled:false,message:'");
                writer.write("canceled failed for " + kv.getValue("uuid"));
                status = 500;
            }
            if (jsonp) writer.write("'}");
            endResponse(writer, cbf);
            response.setStatus(status);
        } else if (target.equals("/query/encode")) {
            Query q = new Query(null, kv.getValue("query", kv.getValue("path", "")), null);
            JSONArray path = CodecJSON.encodeJSON(q).getJSONArray("path");
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(path.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/query/decode")) {
            String qo = "{path:" + kv.getValue("query", kv.getValue("path", "")) + "}";
            Query q = CodecJSON.decodeString(new Query(), qo);
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(q.getPaths()[0]);
            endResponse(writer, cbf);
        } else if (target.equals("/v2/queries/finished.list")) {
            JSONArray runningEntries = new JSONArray();
            for (QueryTracker.QueryEntryInfo entryInfo : tracker.getCompleted()) {
                JSONObject entryJSON = CodecJSON.encodeJSON(entryInfo);
                //TODO: replace this with some high level summary
                entryJSON.put("hostInfoSet", "");
                runningEntries.put(entryJSON);
            }
            response.setContentType("application/json; charset=utf-8");
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(runningEntries.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/queries/running.list")) {
            JSONArray runningEntries = new JSONArray();
            for (QueryTracker.QueryEntryInfo entryInfo : tracker.getRunning()) {
                JSONObject entryJSON = CodecJSON.encodeJSON(entryInfo);
                //TODO: replace this with some high level summary
                entryJSON.put("hostInfoSet", "");
                runningEntries.put(entryJSON);
            }
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(runningEntries.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/queries/list")) {
            JSONArray queries = new JSONArray();
            for (QueryTracker.QueryEntryInfo entryInfo : tracker.getCompleted()) {
                JSONObject entryJSON = CodecJSON.encodeJSON(entryInfo);
                entryJSON.put("hostEntries", entryInfo.hostInfoSet.size());
                entryJSON.put("state", 0);
                queries.put(entryJSON);
            }
            for (QueryTracker.QueryEntryInfo entryInfo : tracker.getQueued()) {
                JSONObject entryJSON = CodecJSON.encodeJSON(entryInfo);
                entryJSON.put("hostEntries", entryInfo.hostInfoSet.size());
                entryJSON.put("state", 2);
                queries.put(entryJSON);
            }
            for (QueryTracker.QueryEntryInfo entryInfo : tracker.getRunning()) {
                JSONObject entryJSON = CodecJSON.encodeJSON(entryInfo);
                entryJSON.put("hostEntries", entryInfo.hostInfoSet.size());
                entryJSON.put("state", 3);
                queries.put(entryJSON);
            }
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(queries.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/job/list")) {
            StringWriter swriter = new StringWriter();
            final JsonGenerator json = factory.createJsonGenerator(swriter);
            json.writeStartArray();
            for (IJob job : meshQueryMaster.getJobs()) {
                if (job.getQueryConfig() != null && job.getQueryConfig().getCanQuery()) {
                    List<JobTask> tasks = job.getCopyOfTasks();
                    String uuid = job.getId();
                    json.writeStartObject();
                    json.writeStringField("id", uuid);
                    json.writeStringField("description", Optional.fromNullable(job.getDescription()).or(""));
                    json.writeNumberField("state", job.getState().ordinal());
                    json.writeStringField("creator", job.getCreator());
                    json.writeNumberField("submitTime", Optional.fromNullable(job.getSubmitTime()).or(-1L));
                    json.writeNumberField("startTime", Optional.fromNullable(job.getStartTime()).or(-1L));
                    json.writeNumberField("endTime", Optional.fromNullable(job.getStartTime()).or(-1L));
                    json.writeNumberField("replicas", Optional.fromNullable(job.getReplicas()).or(0));
                    json.writeNumberField("backups", Optional.fromNullable(job.getBackups()).or(0));
                    json.writeNumberField("nodes", tasks.size());
                    json.writeEndObject();
                }
            }
            json.writeEndArray();
            json.close();
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(swriter.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/host/list")) {
            StringWriter swriter = new StringWriter();
            final JsonGenerator json = factory.createJsonGenerator(swriter);
            json.writeStartArray();
            for (HostEntryInfo hostEntryInfo : tracker.getQueryHosts(kv.getValue("uuid"))) {
                json.writeStartObject();
                json.writeStringField("hostname", hostEntryInfo.getHostName());
                json.writeNumberField("lines", hostEntryInfo.getLines());
                json.writeNumberField("startTime", hostEntryInfo.getStarttime());
                json.writeNumberField("endTime", hostEntryInfo.getEndtime());
                json.writeNumberField("taskId", hostEntryInfo.getTaskId());
                json.writeBooleanField("finished", hostEntryInfo.getFinished());
                json.writeEndObject();
            }
            json.writeEndArray();
            json.close();
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(swriter.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/settings/git.properties")) {
            StringWriter swriter = new StringWriter();
            final JsonGenerator json = factory.createJsonGenerator(swriter);
            Properties gitProperties = new Properties();
            json.writeStartObject();
            try {
                InputStream in = getClass().getResourceAsStream("/git.properties");
                gitProperties.load(in);
                in.close();
                json.writeStringField("commitIdAbbrev", gitProperties.getProperty("git.commit.id.abbrev"));
                json.writeStringField("commitUserEmail", gitProperties.getProperty("git.commit.user.email"));
                json.writeStringField("commitMessageFull", gitProperties.getProperty("git.commit.message.full"));
                json.writeStringField("commitId", gitProperties.getProperty("git.commit.id"));
                json.writeStringField("commitUserName", gitProperties.getProperty("git.commit.user.name"));
                json.writeStringField("buildUserName", gitProperties.getProperty("git.build.user.name"));
                json.writeStringField("commitIdDescribe", gitProperties.getProperty("git.commit.id.describe"));
                json.writeStringField("buildUserEmail", gitProperties.getProperty("git.build.user.email"));
                json.writeStringField("branch", gitProperties.getProperty("git.branch"));
                json.writeStringField("commitTime", gitProperties.getProperty("git.commit.time"));
                json.writeStringField("buildTime", gitProperties.getProperty("git.build.time"));
            } catch (Exception ex) {
                log.warn("Error loading git.properties, possibly jar was not compiled with maven.");
            }
            json.writeEndObject();
            json.close();
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(swriter.toString());
            endResponse(writer, cbf);
        } else if (target.equals("/v2/hosts/list")) {
            String hosts = meshQueryMaster.getMeshHostJSON();
            PrintWriter writer = startResponse(response, cbf, cba);
            writer.write(hosts);
            endResponse(writer, cbf);
        }
//      else if (target.equals("/monitors.rescan"))
//      {
//          updateDbMap();
//          log.emit("[monitors.scan] requested from " + request.getRemoteAddr());
//          response.getWriter().write("Updating Monitored Hosts");
//          writeState();
//      }
//      else if (target.equals("/kill/queryworker"))
//      {
//          String host = kv.getValue("host");
//          if (host != null)
//          {
//              sendPoisonPill(host);
//              response.getWriter().write("Sent poison pill to: " + host);
//          }
//      }
        else {
            File file = new File(webDir, target);
            if (file.exists() && file.isFile()) {
                OutputStream out = response.getOutputStream();
                InputStream in = new FileInputStream(file);
                byte buf[] = new byte[1024];
                int read = 0;
                while ((read = in.read(buf)) >= 0) {
                    out.write(buf, 0, read);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[http.unhandled] " + target);
                }
                response.sendError(404);
            }
        }
        if (request instanceof Request) {
            ((Request) request).setHandled(handled);
        }
    }

}

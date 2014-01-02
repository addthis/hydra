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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.CUID;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.ErrorHandlingQuerySource;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.util.StringMapHelper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.CharEncoding;

import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class QueryServlet {

    private static final Logger log = LoggerFactory.getLogger(QueryServlet.class);

    private static final int maxQueryTime = Parameter.intValue("qmaster.maxQueryTime", 24 * 60 * 60); // one day
    private static String asyncType = Parameter.value("qmaster.async.type", "cuid");


    // You have 5 minutes to claim your async result, if we ever need to
    // parametrize this we have created a monster.
    private static final Cache<String, Query> asyncCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    private static final char hex[] = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static final Timer queryTimes = Metrics.newTimer(QueryServlet.class, "queryTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    /* convert string to json valid format */
    public static final String jsonEncode(String s) {
        char ca[] = s.toCharArray();
        int alt = 0;
        for (int i = 0; i < ca.length; i++) {
            if (ca[i] < 48 || ca[i] > 90) {
                alt++;
            }
        }
        if (alt == 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(ca.length + alt * 3);
        for (int i = 0; i < ca.length; i++) {
            char c = ca[i];
            if (c > 47 && c < 91) {
                sb.append(ca[i]);
            } else {
                switch (ca[i]) {
                    case '"':
                        sb.append("\\\"");
                        break;
                    case '\\':
                        sb.append("\\\\");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    default:
                        if (c < 32 || c > 255) {
                            sb.append("\\u");
                            long v = c;
                            char cb[] = new char[4];
                            for (int j = 0; j < 4; j++) {
                                cb[3 - j] = hex[(int) (v & 0xf)];
                                v >>= 4;
                            }
                            sb.append(cb);
                        } else {
                            sb.append(c);
                        }
                        break;
                }
            }
        }
        return sb.toString();
    }

    /**
     * KV helper
     */
    public static KVPairs requestToVKPairs(HttpServletRequest request) {
        KVPairs kv = new KVPairs();
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements();) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        return kv;
    }

    /**
     * special handler for query
     */
    public static void handleQuery(QuerySource querySource, KVPairs kv, HttpServletRequest request, HttpServletResponse response) throws Exception {
        String job = kv.getValue("job");
        String path = kv.getValue("path", kv.getValue("q", ""));
        Query query = new Query(job, new String[] { path }, new String[] { kv.getValue("ops"), kv.getValue("rops") });
        query.setTraced(kv.getIntValue("trace", 0) == 1);
        handleQuery(querySource, query, kv, request, response);
    }

    /** */
    public static void handleQuery(QuerySource querySource, Query query, KVPairs kv, HttpServletRequest request, HttpServletResponse response) throws Exception {
        query.setParameterIfNotYetSet("hosts", kv.getValue("hosts"));
        query.setParameterIfNotYetSet("gate", kv.getValue("gate"));
        query.setParameterIfNotYetSet("cache", kv.getValue("cache"));
        query.setParameterIfNotYetSet("nocache", kv.getValue("nocache"));
        query.setParameterIfNotYetSet("dontcacheme", kv.getValue("dontcacheme"));
        query.setParameterIfNotYetSet("originalrequest", kv.getValue("originalrequest"));
        query.setParameterIfNotYetSet("remoteip", request.getRemoteAddr());
        query.setParameterIfNotYetSet("parallel", kv.getValue("parallel"));
        query.setParameterIfNotYetSet("allowPartial", kv.getValue("allowPartial"));
        query.setParameterIfNotYetSet("cachettl", kv.getValue("cachettl"));
        query.setParameterIfNotYetSet("dsortcompression", kv.getValue("dsortcompression"));

        String filename = kv.getValue("filename", "query");
        String format = kv.getValue("format", "json");
        String async = kv.getValue("async");
        String jsonp = kv.getValue("jsonp");
        String jargs = kv.getValue("jargs");

        int timeout = Math.min(kv.getIntValue("timeout", maxQueryTime), maxQueryTime);
        query.setParameterIfNotYetSet("timeout", timeout);
        query.setParameter("sender", kv.getValue("sender"));

        if (log.isDebugEnabled()) {
            log.debug(new StringMapHelper()
                    .put("type", "query.starting")
                    .put("query.path", query.getPaths()[0])
                    .put("query.hosts", query.getParameter("hosts"))
                    .put("query.ops", query.getOps())
                    .put("trace", query.isTraced())
                    .put("cachettl", query.getCacheTTL())
                    .put("nocache", query.getParameter("nocache"))
                    .put("dontcacheme", query.getParameter("dontcacheme"))
                    .put("cache.gate", query.getParameter("gate"))
                    .put("sources", query.getParameter("sources"))
                    .put("time", System.currentTimeMillis())
                    .put("job.id", query.getJob())
                    .put("query.id", query.uuid())
                    .put("sender", query.getParameter("sender"))
                    .put("async", async)
                    .put("format", format)
                    .put("filename", filename)
                    .put("originalrequest", query.getParameter("originalrequest"))
                    .put("timeout", query.getParameter("timeout"))
                    .put("requestIP", query.getParameter("remoteip"))
                    .put("parallel", query.getParameter("parallel"))
                    .put("allowPartial", query.getParameter("allowPartial")).createKVPairs().toString());
        }
        QueryHandle queryHandle = null;
        try {
            response.setCharacterEncoding(CharEncoding.UTF_8);
            ServletConsumer consumer = null;
            String asyncUuid = null;
            if (async != null) {
                if (async.equals("new")) {
                    asyncUuid = genAsyncUuid(query);
                    asyncCache.put(asyncUuid, query.createClone());
                    if (query.isTraced()) {
                        Query.emitTrace("async create " + asyncUuid + " from " + query);
                    }
                    format = "async";
                } else {
                    Query asyncQuery = asyncCache.getIfPresent(async);
                    asyncCache.invalidate(async);
                    if (query.isTraced()) {
                        Query.emitTrace("async restore " + async + " as " + asyncQuery);
                    }
                    if (asyncQuery != null) {
                        query = asyncQuery;
                    }
                }
            }
            if (!format.equals("async")) {
                if (query.getJob() == null) {
                    response.sendError(500, "missing job");
                    return;
                }
            }
            if (format.equals("async")) {
                consumer = new OutputAsync(response, asyncUuid);
            } else if (format.equals("json")) {
                consumer = new OutputJson(response, jsonp, jargs);
            } else if (format.equals("html")) {
                consumer = new OutputHTML(response);
            } else {
                consumer = OutputDelimited.create(response, filename, format);
            }
            if (consumer != null) {
                queryHandle = querySource.query(query, consumer);
                consumer.waitDone(timeout);
                if (consumer.isError()) {
                    handleError(querySource, query);
                }
            } else {
                response.sendError(400, "Invalid format");
            }
            ((Request) request).setHandled(true);
        } catch (IOException e) {
            if (jsonp != null) {
                response.getWriter().write(jsonp + "(" + (jargs != null ? jargs + "," : "") + "{error:'" + e + "'});");
            } else {
                response.sendError(500, "General Error " + e.toString());
            }
            if (queryHandle != null) {
                queryHandle.cancel(e.getMessage());
            }
            handleError(querySource, query);
        } catch (QueryException e) {
            if (jsonp != null) {
                response.getWriter().write(jsonp + "(" + (jargs != null ? jargs + "," : "") + "{error:'" + e.getMessage() + "'});");
            } else {
                response.sendError(500, "Query Error " + e.toString());
            }
            if (queryHandle != null) {
                queryHandle.cancel(e.getMessage());
            }
            handleError(querySource, query);
        }
    }

    private static void handleError(QuerySource source, Query query) {
        if (source instanceof ErrorHandlingQuerySource) {
            ((ErrorHandlingQuerySource) source).handleError(query);
        }
    }

    private static String genAsyncUuid(Query query) {
        if (asyncType.equals("uuid")) {
            return UUID.randomUUID().toString();
        } else if (asyncType.equals("shaorig") && !Strings.isEmpty(query.getParameter("originalrequest"))) {
            return DigestUtils.shaHex(query.getParameter("originalrequest"));
        }
        return CUID.createCUID();
    }

    /**
     * parent of all streaming response classes
     */
    private abstract static class ServletConsumer implements DataChannelOutput {

        HttpServletResponse response;
        PrintWriter writer;
        final Semaphore gate = new Semaphore(1);
        final AtomicBoolean done = new AtomicBoolean(false);
        final ListBundleFormat format = new ListBundleFormat();
        final long startTime;
        boolean error = false;

        ServletConsumer(HttpServletResponse response) throws IOException, InterruptedException {
            this.response = response;
            this.writer = response.getWriter();
            startTime = System.currentTimeMillis();
            gate.acquire();
        }

        void setDone() {
            if (done.compareAndSet(false, true)) {
                gate.release();
            }
        }

        void waitDone() throws InterruptedException, IOException {
            waitDone(maxQueryTime);
        }

        void waitDone(final int waitInSeconds) throws InterruptedException, IOException {
            if (!done.get()) {
                try {
                    gate.acquire();
                } finally {
                    setDone();
                }
            }
        }

        @Override
        public void sourceError(DataChannelError ex) {
            try {
                response.getWriter().write(ex.getMessage());
                response.setStatus(500);
                error = true;
                log.error("", ex);
            } catch (IOException e)  {
                log.warn("", "Exception sending error: " + e);
            } finally {
                setDone();
            }
        }

        @Override
        public Bundle createBundle() {
            return new ListBundle(format);
        }

        protected boolean isError() {
            return error;
        }
    }

    /**
     * for async response
     */
    private static class OutputAsync extends ServletConsumer {

        OutputAsync(HttpServletResponse response, String asyncid) throws IOException, InterruptedException {
            super(response);
            response.getWriter().write("{\"id\":\"" + asyncid + "\"}");
            setDone();
        }

        @Override
        public void send(Bundle row) {
        }

        @Override
        public void send(List<Bundle> bundles) {
        }

        @Override
        public void sendComplete() {
        }
    }

    /** */
    private static class OutputJson extends ServletConsumer {

        int rows = 0;
        private String jsonp;

        OutputJson(HttpServletResponse response, String jsonp, String jargs) throws IOException, InterruptedException {
            super(response);
            this.jsonp = jsonp;
            response.setContentType("application/json; charset=utf-8");
            if (jsonp != null) {
                writer.write(jsonp);
                writer.write("(");
                if (jargs != null) {
                    writer.write(jargs);
                    writer.write(",");
                }
            }
            writer.write("[");
        }

        @Override
        public synchronized void send(Bundle row) {
            if (rows++ > 0) {
                writer.write(",");
            }
            writer.write("[");
            int count = 0;
            for (BundleField field : row.getFormat()) {
                ValueObject o = row.getValue(field);
                if (count++ > 0) {
                    writer.write(",");
                }
                if (o == null) {
                    continue;
                }
                ValueObject.TYPE type = o.getObjectType();
                if (type == ValueObject.TYPE.CUSTOM) {
                    o = o.asCustom().asSimple();
                    type = o.getObjectType();
                }
                switch (type) {
                    case INT:
                    case FLOAT:
                        writer.write(o.toString());
                        break;
                    case STRING:
                        writer.write('"');
                        writer.write(jsonEncode(o.toString()));
                        writer.write('"');
                        break;
                    default:
                        break;
                }
            }
            writer.write("]");
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                for (Bundle bundle : bundles) {
                    send(bundle);
                }
            }
        }

        @Override
        public void sendComplete() {
            writer.write("]");
            if (jsonp != null) {
                writer.write(");");
            }
            queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            setDone();
        }
    }

    /** */
    private static class OutputDelimited extends ServletConsumer {

        String delimiter;

        OutputDelimited(HttpServletResponse response, String filename, String delimiter) throws IOException, InterruptedException {
            super(response);
            this.delimiter = delimiter;
            response.setContentType("application/csv; charset=utf-8");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        }

        public static OutputDelimited create(HttpServletResponse response, String filename, String format) throws IOException, InterruptedException {
            String delimiter;
            if (format.equals("tsv")) {
                delimiter = "\t";
            } else if (format.equals("csv")) {
                delimiter = ",";
            } else if (format.equals("psv")) {
                delimiter = "|";
            } else {
                return null;
            }
            if (!filename.toLowerCase().endsWith("." + format)) {
                filename = filename.concat("." + format);
            }
            return new OutputDelimited(response, filename, delimiter);
        }

        @Override
        public synchronized void send(Bundle row) {
            int count = 0;
            for (BundleField field : row.getFormat()) {
                ValueObject o = row.getValue(field);
                if (count++ > 0) {
                    writer.write(delimiter);
                }
                if (o != null) {
                    ValueObject.TYPE type = o.getObjectType();
                    if (type == ValueObject.TYPE.CUSTOM) {
                        o = o.asCustom().asSimple();
                        type = o.getObjectType();
                    }
                    switch (type) {
                        case INT:
                        case FLOAT:
                            writer.write(o.toString());
                            break;
                        case STRING:
                            writer.write('"');
                            writer.write(o.toString().replace('"', '\'').replace('\n', ' ').replace('\r', ' '));
                            writer.write('"');
                            break;
                        default:
                            break;
                    }
                }
            }
            writer.write("\n");
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                for (Bundle bundle : bundles) {
                    send(bundle);
                }
            }
        }

        @Override
        public void sendComplete() {
            queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            setDone();
        }
    }

    /** */
    private static class OutputHTML extends ServletConsumer {

        OutputHTML(HttpServletResponse response) throws IOException, InterruptedException {
            super(response);
            response.setContentType("text/html; charset=utf-8");
            writer.write("<table border=1 cellpadding=1 cellspacing=0>\n");
        }

        @Override
        public synchronized void send(Bundle row) {
            writer.write("<tr>");
            for (BundleField field : row.getFormat()) {
                ValueObject o = row.getValue(field);
                writer.write("<td>" + o + "</td>");
            }
            writer.write("</tr>\n");
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                for (Bundle bundle : bundles) {
                    send(bundle);
                }
            }
        }

        @Override
        public void sendComplete() {
            writer.write("</table>");
            queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            setDone();
        }
    }
}

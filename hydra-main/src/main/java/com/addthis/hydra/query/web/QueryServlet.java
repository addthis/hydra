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

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.ErrorHandlingQuerySource;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.util.StringMapHelper;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;

import org.apache.commons.lang3.CharEncoding;

import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

public final class QueryServlet {

    private static final Logger log = LoggerFactory.getLogger(QueryServlet.class);

    static final int maxQueryTime = Parameter.intValue("qmaster.maxQueryTime", 24 * 60 * 60); // one day

    static final char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    static final Timer queryTimes = Metrics.newTimer(QueryServlet.class, "queryTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    private QueryServlet() {
    }

    /* convert string to json valid format */
    public static String jsonEncode(String s) {
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
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements(); ) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        return kv;
    }

    /**
     * special handler for query
     */
    public static void handleQuery(QuerySource querySource, KVPairs kv, HttpRequest request,
            ChannelHandlerContext ctx) throws Exception {
        String job = kv.getValue("job");
        String path = kv.getValue("path", kv.getValue("q", ""));
        Query query = new Query(job, new String[]{path}, new String[]{kv.getValue("ops"), kv.getValue("rops")});
        query.setTraced(kv.getIntValue("trace", 0) == 1);
        handleQuery(querySource, query, kv, request, ctx);
    }

    public static void handleQuery(QuerySource querySource, Query query, KVPairs kv, HttpRequest request,
            ChannelHandlerContext ctx) throws Exception {
        query.setParameterIfNotYetSet("hosts", kv.getValue("hosts"));
        query.setParameterIfNotYetSet("gate", kv.getValue("gate"));
        query.setParameterIfNotYetSet("originalrequest", kv.getValue("originalrequest"));
        SocketAddress remoteIP = ctx.channel().remoteAddress();
        if (remoteIP instanceof InetSocketAddress) { // only log implementations with known methods
            query.setParameterIfNotYetSet("remoteip", ((InetSocketAddress) remoteIP).getAddress().getHostAddress());
        }
        query.setParameterIfNotYetSet("parallel", kv.getValue("parallel"));
        query.setParameterIfNotYetSet("allowPartial", kv.getValue("allowPartial"));
        query.setParameterIfNotYetSet("dsortcompression", kv.getValue("dsortcompression"));

        String filename = kv.getValue("filename", "query");
        String format = kv.getValue("format", "json");
        String jsonp = kv.getValue("jsonp", kv.getValue("cbfunc"));
        String jargs = kv.getValue("jargs", kv.getValue("cbfunc-arg"));

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
                    .put("sources", query.getParameter("sources"))
                    .put("time", System.currentTimeMillis())
                    .put("job.id", query.getJob())
                    .put("query.id", query.uuid())
                    .put("sender", query.getParameter("sender"))
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

            // support legacy async query semantics
//            query = LegacyHandler.handleQuery(query, kv, request, response);
            if (query == null) {
                return;
            }

            if (query.getJob() == null) {
                response.sendError(500, "missing job");
                return;
            }
            ServletConsumer consumer = null;
            switch (format) {
                case "json":
                    consumer = new OutputJson(response, jsonp, jargs);
                    break;
                case "html":
                    consumer = new OutputHTML(response);
                    break;
                default:
                    consumer = OutputDelimited.create(response, filename, format);
                    break;
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
}

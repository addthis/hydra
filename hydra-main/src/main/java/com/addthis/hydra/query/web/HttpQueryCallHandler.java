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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.source.ErrorHandlingQuerySource;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.util.StringMapHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.query.web.HttpUtils.sendError;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.EventExecutor;

public final class HttpQueryCallHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpQueryCallHandler.class);

    private static final StringEncoder stringer = new StringEncoder(CharsetUtil.UTF_8);

    static final int maxQueryTime = Parameter.intValue("qmaster.maxQueryTime", 24 * 60 * 60); // one day

    private HttpQueryCallHandler() {
    }

    /**
     * special handler for query
     */
    public static ChannelFuture handleQuery(ChannelHandler queryToQueryResultsEncoder, KVPairs kv,
            HttpRequest request, ChannelHandlerContext ctx, EventExecutor executor) throws Exception {
        String job = kv.getValue("job");
        String path = kv.getValue("path", kv.getValue("q", ""));
        Query query = new Query(job, new String[]{path}, new String[]{kv.getValue("ops"), kv.getValue("rops")});
        query.setTraced(kv.getIntValue("trace", 0) == 1);
        query.setParameterIfNotYetSet("hosts", kv.getValue("hosts"));
        query.setParameterIfNotYetSet("gate", kv.getValue("gate"));
        query.setParameterIfNotYetSet("originalrequest", kv.getValue("originalrequest"));
        SocketAddress remoteIP = ctx.channel().remoteAddress();
        if (remoteIP instanceof InetSocketAddress) { // only log implementations with known methods
            query.setParameterIfNotYetSet("remoteip", ((InetSocketAddress) remoteIP).getAddress().getHostAddress());
        }
        query.setParameterIfNotYetSet("allocator", kv.getValue("allocator"));
        query.setParameterIfNotYetSet("allowPartial", kv.getValue("allowPartial"));

        String filename = kv.getValue("filename", "query");
        String format = kv.getValue("format", "json");
        String gdriveAccessToken = kv.getValue("accesstoken");
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
                    .put("allocator", query.getParameter("allocator"))
                    .put("allowPartial", query.getParameter("allowPartial")).createKVPairs().toString());
        }
        // support legacy async query semantics
        query = LegacyHandler.handleQuery(query, kv, request, ctx);
        if (query == null) {
            return ctx.newSucceededFuture();
        }

        if (query.getJob() == null) {
            sendError(ctx, new HttpResponseStatus(500, "missing job"));
            return ctx.newSucceededFuture();
        }
        switch (format) {
            case "json":
                ctx.pipeline().addLast(executor, "format", new JsonBundleEncoder());
                break;
            case "html":
                ctx.pipeline().addLast(executor, "format", new HtmlBundleEncoder());
                break;
            case "gdrive":
                ctx.pipeline().addLast(executor, "stringer", stringer);
                ctx.pipeline().addLast(executor, "format",
                        GoogleDriveBundleEncoder.create(filename, gdriveAccessToken));
                break;
            default:
                ctx.pipeline().addLast(executor,
                        "format", DelimitedBundleEncoder.create(filename, format));
                break;
        }
        ctx.pipeline().addLast(executor, "mqm", queryToQueryResultsEncoder);
        return ctx.pipeline().write(query, new DefaultChannelProgressivePromise(ctx.channel(), executor));
    }

    private static void handleError(QuerySource source, Query query) {
        if (source instanceof ErrorHandlingQuerySource) {
            ((ErrorHandlingQuerySource) source).handleError(query);
        }
    }
}

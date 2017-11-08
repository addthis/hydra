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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import java.nio.CharBuffer;

import com.addthis.basis.kv.KVPairs;

import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.loadbalance.QueryQueue;
import com.addthis.hydra.query.loadbalance.WorkerData;
import com.addthis.hydra.query.tracker.DetailedStatusHandler;
import com.addthis.hydra.query.tracker.QueryEntry;
import com.addthis.hydra.query.tracker.QueryEntryInfo;
import com.addthis.hydra.query.tracker.QueryTracker;
import com.addthis.hydra.util.MetricsServletShim;
import com.addthis.maljson.JSONArray;

import com.typesafe.config.ConfigFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.apache.commons.io.output.StringBuilderWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.Future;

import static com.addthis.hydra.query.web.HttpUtils.sendError;
import static com.addthis.hydra.query.web.HttpUtils.sendRedirect;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.stream.Collectors.toMap;

@ChannelHandler.Sharable
public class HttpQueryHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(HttpQueryHandler.class);

    /**
     * Used for tracking metrics and other interesting things about queries that we have run.  Provides insight
     * into currently running queries and provides the ability to cancel a query before it completes.
     */
    private final QueryTracker tracker;

    /** primary query source */
    private final MeshQueryMaster meshQueryMaster;

    private final QueryQueue queryQueue;
    private final HttpStaticFileHandler staticFileHandler;
    private final MetricsServletShim fakeMetricsServlet;

    // http metrics; may use other classes to derive metric paths for legacy metric namespace consistency
    private final Counter rawQueryCalls = Metrics.newCounter(MeshQueryMaster.class, "rawQueryCalls");

    public HttpQueryHandler(QueryTracker tracker, MeshQueryMaster meshQueryMaster, QueryQueue queryQueue) {
        super(true); // auto release
        this.tracker = tracker;
        this.meshQueryMaster = meshQueryMaster;
        this.queryQueue = queryQueue;
        this.fakeMetricsServlet = new MetricsServletShim();
        this.staticFileHandler = new HttpStaticFileHandler();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("Exception caught while serving http query endpoint", cause);
        if (ctx.channel().isActive()) {
            sendError(ctx, new HttpResponseStatus(500, cause.getMessage()));
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        messageReceived(ctx, request); // redirect to more sensible netty5 naming scheme
    }

    private static void decodeParameters(QueryStringDecoder urlDecoder, KVPairs kv) {
        for (Map.Entry<String, List<String>> entry : urlDecoder.parameters().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue().get(0); // ignore duplicates
            kv.add(k, v);
        }
    }

    protected void messageReceived(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (!request.getDecoderResult().isSuccess()) {
            sendError(ctx, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        QueryStringDecoder urlDecoder = new QueryStringDecoder(request.getUri());
        String target = urlDecoder.path();
        if (request.getMethod() == HttpMethod.POST) {
            log.trace("POST Method handling triggered for {}", request);
            String postBody = request.content().toString(UTF_8);
            log.trace("POST body {}", postBody);
            urlDecoder = new QueryStringDecoder(postBody, false);
        }
        log.trace("target uri {}", target);
        KVPairs kv = new KVPairs();
        /**
         * The "/query/google/submit" endpoint needs to unpack the
         * "state" parameter into KV pairs.
         */
        if (target.equals("/query/google/submit")) {
            String state = urlDecoder.parameters().get("state").get(0);
            QueryStringDecoder newDecoder = new QueryStringDecoder(state, false);
            decodeParameters(newDecoder, kv);
            if (urlDecoder.parameters().containsKey("code")) {
                kv.add(GoogleDriveAuthentication.authtoken, urlDecoder.parameters().get("code").get(0));
            }
            if (urlDecoder.parameters().containsKey("error")) {
                kv.add(GoogleDriveAuthentication.autherror, urlDecoder.parameters().get("error").get(0));
            }
        } else {
            decodeParameters(urlDecoder, kv);
        }
        log.trace("kv pairs {}", kv);
        switch (target) {
            case "/": {
                sendRedirect(ctx, "/query/index.html");
                break;
            }
            case "/q/": {
                sendRedirect(ctx, "/query/call?" + kv);
                break;
            }
            case "/query/call":
            case "/query/call/": {
                rawQueryCalls.inc();
                queryQueue.queueQuery(meshQueryMaster, kv, request, ctx);
                break;
            }
            case "/query/google/authorization": {
                GoogleDriveAuthentication.gdriveAuthorization(kv, ctx);
                break;
            }
            case "/query/google/submit": {
                boolean success = GoogleDriveAuthentication.gdriveAccessToken(kv, ctx);
                if (success) {
                    queryQueue.queueQuery(meshQueryMaster, kv, request, ctx);
                }
                break;
            }
            default:
                fastHandle(ctx, request, target, kv);
                break;
        }
    }

    private void fastHandle(ChannelHandlerContext ctx, FullHttpRequest request, String target, KVPairs kv)
            throws Exception {
        StringBuilderWriter writer = new StringBuilderWriter(50);
        HttpResponse response = HttpUtils.startResponse(writer);
        response.headers().add("Access-Control-Allow-Origin", "*");

        switch (target) {
            case "/metrics": {
                fakeMetricsServlet.writeMetrics(writer, kv);
                break;
            }
            case "/running":
            case "/query/list":
            case "/query/running":
            case "/v2/queries/running.list": {
                Jackson.defaultMapper().writerWithDefaultPrettyPrinter().writeValue(writer, tracker.getRunning());
                break;
            }
            case "/done":
            case "/complete":
            case "/query/done":
            case "/query/complete":
            case "/completed/list":
            case "/v2/queries/finished.list": {
                Jackson.defaultMapper().writerWithDefaultPrettyPrinter().writeValue(writer, tracker.getCompleted());
                break;
            }
            case "/query/all":
            case "/v2/queries/list": {
                Collection<QueryEntryInfo> aggregatingSnapshot = tracker.getRunning();
                aggregatingSnapshot.addAll(tracker.getCompleted());
                Jackson.defaultMapper().writerWithDefaultPrettyPrinter().writeValue(writer, aggregatingSnapshot);
                break;
            }
            case "/cancel":
            case "/query/cancel": {
                if (tracker.cancelRunning(kv.getValue("uuid"))) {
                    writer.write("canceled " + kv.getValue("uuid"));
                } else {
                    writer.write("canceled failed for " + kv.getValue("uuid"));
                    response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                }
                break;
            }
            case "/workers":
            case "/query/workers":
            case "/v2/queries/workers": {
                Map<String, Integer> workerSnapshot = meshQueryMaster.worky().values().stream().collect(
                        toMap(WorkerData::hostName, WorkerData::queryLeases));
                Jackson.defaultMapper().writerWithDefaultPrettyPrinter().writeValue(writer, workerSnapshot);
                break;
            }
            case "/host":
            case "/host/list":
            case "/v2/host/list":
                String queryStatusUuid = kv.getValue("uuid");
                QueryEntry queryEntry = tracker.getQueryEntry(queryStatusUuid);
                if (queryEntry != null) {
                    DetailedStatusHandler hostDetailsHandler =
                            new DetailedStatusHandler(writer, response, ctx, request, queryEntry);
                    hostDetailsHandler.handle();
                    return;
                } else {
                    QueryEntryInfo queryEntryInfo = tracker.getCompletedQueryInfo(queryStatusUuid);
                    if (queryEntryInfo != null) {
                        Jackson.defaultMapper().writerWithDefaultPrettyPrinter().writeValue(writer, queryEntryInfo);
                    } else {
                        log.trace("could not find query for status");
                        if (ctx.channel().isActive()) {
                            sendError(ctx, new HttpResponseStatus(NOT_FOUND.code(), "could not find query"));
                        }
                        return;
                    }
                    break;
                }
            case "/git":
            case "/v2/settings/git.properties": {
                try {
                    Jackson.defaultMapper().writeValue(
                            writer, ConfigFactory.parseResourcesAnySyntax("/hydra-git.properties").getConfig("git"));
                } catch (Exception ex) {
                    String noGitWarning = "Error loading git.properties, possibly jar was not compiled with maven.";
                    log.warn(noGitWarning);
                    writer.write(noGitWarning);
                }
                break;
            }
            case "/query/encode": {
                Query q = new Query(null,
                                    new String[]{kv.getValue("query", kv.getValue("path", ""))},
                                    null);
                JSONArray path = CodecJSON.encodeJSON(q).getJSONArray("path");
                writer.write(path.toString());
                break;
            }
            case "/query/decode": {
                String qo = "{path:" + kv.getValue("query", kv.getValue("path", "")) + "}";
                Query q = CodecJSON.decodeString(Query.class, qo);
                writer.write(q.getPaths()[0]);
                break;
            }
            case "/mqmaster/quiesce": {
                log.trace("Received MeshQueryMaster quiesce request");
                String quiesce = kv.getValue("quiesce");
                switch (quiesce) {
                    case "1":   meshQueryMaster.quiesceMqMaster(true);
                        break;
                    case "0":   meshQueryMaster.quiesceMqMaster(false);
                        break;
                    default:    writer.write("Bad request");
                }
                break;
            }
            case "/mqmaster/healthcheck": {
                if(meshQueryMaster.getIsQuiesced().get()) {
                    writer.write("0");
                } else {
                    writer.write("1");
                }
                break;
            }
            default:
                // forward to static file server
                ctx.pipeline().addLast(staticFileHandler);
                request.retain();
                ctx.fireChannelRead(request);
                return; // don't do text response clean up
        }
        log.trace("response being sent {}", writer);
        ByteBuf textResponse = ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap(writer.getBuilder()), UTF_8);
        HttpContent content = new DefaultHttpContent(textResponse);
        response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, textResponse.readableBytes());
        if (HttpHeaders.isKeepAlive(request)) {
            response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }
        ctx.write(response);
        ctx.write(content);
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        log.trace("response pending");
        if (!HttpHeaders.isKeepAlive(request)) {
            log.trace("Setting close listener");
            ((Future<Void>) lastContentFuture).addListener(ChannelFutureListener.CLOSE);
        }
    }

}
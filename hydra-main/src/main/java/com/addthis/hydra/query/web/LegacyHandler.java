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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import java.nio.CharBuffer;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.CUID;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.io.output.StringBuilderWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

public class LegacyHandler {

    // You have 5 minutes to claim your async result, if we ever need to
    // parametrize this we have created a monster.
    static final Cache<String, Query> asyncCache =
            CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    public static Query handleQuery(Query query, KVPairs kv,
            HttpRequest request, ChannelHandlerContext ctx) throws IOException, QueryException {

        String async = kv.getValue("async");
        if (async == null) {
            return query;
        } else if (async.equals("new")) {
            StringBuilderWriter writer = new StringBuilderWriter(50);
            HttpResponse response = HttpUtils.startResponse(writer);
            String asyncUuid = genAsyncUuid();
            asyncCache.put(asyncUuid, query);
            if (query.isTraced()) {
                Query.emitTrace("async create " + asyncUuid + " from " + query);
            }
            writer.write("{\"id\":\"" + asyncUuid + "\"}");
            ByteBuf textResponse = ByteBufUtil.encodeString(ctx.alloc(),
                    CharBuffer.wrap(writer.getBuilder()), CharsetUtil.UTF_8);
            HttpContent content = new DefaultHttpContent(textResponse);
            response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, textResponse.readableBytes());
            if (HttpHeaders.isKeepAlive(request)) {
                response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }
            ctx.write(response);
            ctx.write(content);
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
            return null;
        } else {
            Query asyncQuery = asyncCache.getIfPresent(async);
            asyncCache.invalidate(async);
            if (query.isTraced()) {
                Query.emitTrace("async restore " + async + " as " + asyncQuery);
            }
            if (asyncQuery != null) {
                return asyncQuery;
            } else {
                throw new QueryException("Missing Async Id");
            }
        }
    }

    static String genAsyncUuid() {
        return CUID.createCUID();
    }
}

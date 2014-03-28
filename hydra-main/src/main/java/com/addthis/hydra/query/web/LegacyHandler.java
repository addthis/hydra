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
import javax.servlet.http.HttpServletResponse;

import java.util.concurrent.TimeUnit;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.CUID;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.eclipse.jetty.server.Request;

public class LegacyHandler {

    // You have 5 minutes to claim your async result, if we ever need to
    // parametrize this we have created a monster.
    static final Cache<String, Query> asyncCache =
            CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    public static Query handleQuery(Query query, KVPairs kv,
            HttpServletRequest request, HttpServletResponse response) throws Exception {

        String async = kv.getValue("async");
        if (async == null) {
            return query;
        } else if (async.equals("new")) {
            String asyncUuid = genAsyncUuid();
            asyncCache.put(asyncUuid, query);
            if (query.isTraced()) {
                Query.emitTrace("async create " + asyncUuid + " from " + query);
            }
            response.getWriter().write("{\"id\":\"" + asyncUuid + "\"}");
            ((Request) request).setHandled(true);
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

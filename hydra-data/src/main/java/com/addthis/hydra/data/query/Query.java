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
package com.addthis.hydra.data.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.CUID;
import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.mutable.MutableInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * Object representation of a tree query.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Query implements Codable {

    public static final Logger traceLog = LoggerFactory.getLogger("query-trace");

    private static final int    MAX_PRINT_LENGTH = 3000;
    private static final String SESSION_ID       = CUID.createCUID();

    private static final AtomicLong queryIds = new AtomicLong(0);

    @JsonProperty private String[] paths;
    @JsonProperty private String[] ops;
    @JsonProperty private String   job;
    @JsonProperty private boolean  trace;
    @JsonProperty private String   sessionId;
    @JsonProperty private long     queryId;
    @JsonProperty private HashMap<String, String> params = new HashMap<>();

    @JsonIgnore
    public transient ChannelProgressivePromise queryPromise = null;

    private Query() {}

    public Query(String job, String[] paths, String[] ops) {
        this.job = job;
        this.paths = paths;
        this.ops = ops;
        this.sessionId = SESSION_ID;
        this.queryId = queryIds.incrementAndGet();
    }

    public ChannelProgressivePromise getQueryPromise() {
        return queryPromise;
    }

    public static String getPathString(QueryElement... path) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (QueryElement e : path) {
            if (i++ > 0) {
                sb.append("/");
            }
            e.toCompact(sb);
        }
        return sb.toString();
    }

    public static String getShortPathString(QueryElement... path) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (QueryElement e : path) {
            if (i++ > 0) {
                sb.append("/");
            }
            e.toCompact(sb);
        }
        String queryString = sb.toString();
        if (queryString.length() > MAX_PRINT_LENGTH) {
            queryString = queryString.substring(0, MAX_PRINT_LENGTH);
        }
        return queryString;
    }

    public String uuid() {
        return String.valueOf(queryId);
    }

    public long queryId() {
        return queryId;
    }

    public String sessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        try {
            String queryString = CodecJSON.encodeString(this);
            if (queryString != null && queryString.length() > MAX_PRINT_LENGTH) {
                queryString = queryString.substring(0, MAX_PRINT_LENGTH);
            }
            return queryString;
        } catch (Exception ex) {
            return LessStrings.join(paths, "|")
                          .concat(";")
                          .concat(ops != null ? LessStrings.join(ops, "|") : "")
                          .concat(";")
                          .concat(job != null ? job : "");
        }
    }

    public String[] getPaths() {
        return paths;
    }

    public boolean isTraced() {
        return trace;
    }

    public Query setTraced(boolean traced) {
        this.trace = traced;
        return this;
    }

    public List<QueryElement[]> getQueryPaths() {
        ArrayList<QueryElement[]> list = new ArrayList<>(paths.length);
        for (String path : paths) {
            list.add(parseQueryPath(path));
        }
        return list;
    }

    /**
     * turns compact query notation (+:+hits) into an object array
     */
    private static QueryElement[] parseQueryPath(String path) {
        MutableInt column = new MutableInt(0);
        ArrayList<QueryElement> list = new ArrayList<>();
        for (String pe : LessStrings.split(path, "/")) {
            list.add(new QueryElement().parse(pe, column));
        }
        return list.toArray(new QueryElement[list.size()]);
    }

    public QueryOpProcessor newProcessor(DataChannelOutput output, ChannelProgressivePromise opPromise) {
        return new QueryOpProcessor(output, ops, opPromise);
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String[] getOps() {
        return ops;
    }

    /**
     * @return first a query suitable for the next query worker in the stack
     */
    public Query createPipelinedQuery() {
        Query newQuery = cloneTo(new Query());
        if (ops != null && ops.length > 0) {
            String[] newops = new String[ops.length - 1];
            System.arraycopy(ops, 1, newops, 0, newops.length);
            newQuery.ops = newops;
            String pop = ops[0];
            ops = new String[]{pop};
        }
        return newQuery;
    }

    private Query cloneTo(Query q) {
        q.paths = paths;
        q.ops = ops;
        q.job = job;
        q.trace = trace;
        q.params = params;
        q.sessionId = sessionId;
        q.queryId = queryId;
        return q;
    }

    public HashMap<String, String> getParameters() {
        return params;
    }

    public Query setParameterIfNotYetSet(String key, Object value) {
        if (params.get(key) == null) {
            setParameter(key, value);
        }
        return this;
    }

    public Query setParameter(String key, Object value) {
        if (value != null) {
            params.put(key, value.toString());
        } else {
            params.remove(key);
        }
        return this;
    }

    public String getParameter(String key) {
        return getParameter(key, null);
    }

    public String getParameter(String key, String defaultValue) {
        String val = params.get(key);
        return val != null ? val : defaultValue;
    }

    public String removeParameter(String key) {
        return params.remove(key);
    }
}

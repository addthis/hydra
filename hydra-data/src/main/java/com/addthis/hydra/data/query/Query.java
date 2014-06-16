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
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;

import org.apache.commons.lang3.mutable.MutableInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * Object representation of a tree query.
 */
public class Query implements Codec.Codable {

    private static final Logger log              = LoggerFactory.getLogger(Query.class);
    private static final int    MAX_PRINT_LENGTH = 3000;
    private static final String SESSION_ID       = CUID.createCUID();

    private static final AtomicLong queryIds = new AtomicLong(0);

    protected static RollingLog traceLog;

    @Codec.Set(codable = true)
    private String[] paths;
    @Codec.Set(codable = true)
    private String[] ops;
    @Codec.Set(codable = true)
    private String   job;
    @Codec.Set(codable = true)
    private boolean  trace;
    @Codec.Set(codable = true)
    private String   sessionId;
    @Codec.Set(codable = true)
    private long     queryId;
    @Codec.Set(codable = true)
    private HashMap<String, String> params = new HashMap<>();

    @Codec.Set(codable = false)
    public ChannelProgressivePromise queryPromise = null;

    // for codec
    public Query() {
    }

    public Query(String job, String[] paths, String[] ops) {
        this.job = job;
        this.paths = paths;
        this.ops = ops;
        this.sessionId = SESSION_ID;
        this.queryId = queryIds.incrementAndGet();
    }

    //Set the rolling log for trace events
    public static void setTraceLog(RollingLog tLog) {
        traceLog = tLog;
    }

    //write to the log used for query trace events or default to a debug log
    public static void emitTrace(String line) {
        if (traceLog != null) {
            traceLog.writeLine(line);
        } else {
            log.warn(line);
        }
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
            return Strings.join(paths, "|")
                          .concat(";")
                          .concat(ops != null ? Strings.join(ops, "|") : "")
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
        for (String pe : Strings.split(path, "/")) {
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

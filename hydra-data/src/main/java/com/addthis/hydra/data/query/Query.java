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
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.maljson.JSONObject;

import org.apache.commons.lang3.mutable.MutableInt;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Currently only used in the sauron package, but will become more important
 * here.
 */
public class Query implements Codec.Codable {

    private static final Logger log = LoggerFactory.getLogger(Query.class);
    private static final AtomicLong queryID = new AtomicLong(0);
    private static final String idPrefix = CUID.createCUID();

    //  protected static final long maxMem = Parameter.longValue("query.maxmem", 0); // TODO
    protected static final long tipMem = Parameter.longValue("query.tipmem", 0);
    //  protected static final int maxRow = Parameter.intValue("query.maxrow", 0); // TODO
    protected static final int tipRow = Parameter.intValue("query.tiprow", 0);
    protected static RollingLog traceLog;

    private static final int MAX_PRINT_LENGTH = 3000;

    @Codec.Set(codable = false)
    public volatile QueryStatusObserver queryStatusObserver = null;

    //Set the rolling log for trace events
    public static void setTraceLog(RollingLog tLog) {
        traceLog = tLog;
    }

    public static String nextUUID() {
        return idPrefix + ":" + queryID.incrementAndGet();
    }

    //write to the log used for query trace events or default to a debug log
    public static void emitTrace(String line) {
        if (traceLog != null) {
            traceLog.writeLine(line);
        } else {
            log.warn(line);
        }
    }

    /**
     * constructor with VM wide defaults applied
     */
    public static QueryOpProcessor createProcessor(DataChannelOutput output, QueryStatusObserver queryStatusObserver) {
        return createProcessor(output, null, queryStatusObserver);
    }

    public static QueryOpProcessor createProcessor(DataChannelOutput output) {
        return createProcessor(output, null, new QueryStatusObserver());
    }

    public static QueryOpProcessor createProcessor(DataChannelOutput output, String ops[]) {
        return new QueryOpProcessor(output).parseOps(ops).setRowTip(tipRow).setMemTip(tipMem);
    }

    /**
     * constructor with VM wide defaults applied
     */
    public static QueryOpProcessor createProcessor(DataChannelOutput output, String ops[], QueryStatusObserver queryStatusObserver) {
        return new QueryOpProcessor(output, queryStatusObserver).parseOps(ops).setRowTip(tipRow).setMemTip(tipMem);
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

    @Codec.Set(codable = true)
    private String paths[];
    @Codec.Set(codable = true)
    private String ops[];
    @Codec.Set(codable = true)
    private String job;
    @Codec.Set(codable = true)
    private long cachettl;
    @Codec.Set(codable = true)
    private boolean trace;
    @Codec.Set(codable = true)
    private HashMap<String, String> params = new HashMap<>();
    @Codec.Set(codable = true)
    private volatile String uuid = nextUUID();

    private List<QueryOp> appendops = new ArrayList<>(1);

    public Query() {
    }

    public Query(String uuid, String job, String paths[], String ops[]) {
        this(job, paths, ops);
        if (uuid != null) {
            this.uuid = uuid;
        }
    }

    public Query(String job, String path, String ops[]) {
        this(job, new String[]{path}, ops);
    }

    public Query(String job, String paths[], String ops[]) {
        setJob(job);
        setOps(ops);
        setPaths(paths);
    }

    public void useNextUUID() {
        uuid = nextUUID();
    }

    public QueryStatusObserver getQueryStatusObserver() {
        return queryStatusObserver;
    }

    public String getPathString(QueryElement path[]) {
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

    public String getShortPathString(QueryElement path[]) {
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
        return uuid;
    }

    public String hashKey(String exclude) {
        ArrayList<String> xprop = new ArrayList<>(1);
        xprop.add(exclude);
        return hashKey(xprop);
    }

    public String hashKey(List<String> excludeProp) {
        try {
            JSONObject o = CodecJSON.encodeJSON(this);
            o.remove("uuid");
            for (String prop : excludeProp) {
                o.remove(prop);
            }
            return o.toString();
        } catch (Exception ex) {
            return toString();
        }
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
            return Strings.join(paths, "|").concat(";").concat(ops != null ? Strings.join(ops,"|") : "").concat(";").concat(job != null ? job : "");
        }
    }

    /**
     * does not serialize - used internally by query master and worker
     */
    public Query appendOp(QueryOp op) {
        appendops.add(op);
        return this;
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

    public long getCacheTTL() {
        return cachettl;
    }

    public Query setCacheTTL(long ttlms) {
        cachettl = ttlms;
        return this;
    }

    public List<QueryElement[]> getQueryPaths() {
        ArrayList<QueryElement[]> list = new ArrayList<QueryElement[]>(paths.length);
        for (String path : paths) {
            list.add(parseQueryPath(path));
        }
        return list;
    }

    public QueryOpProcessor getProcessor(DataChannelOutput output, QueryStatusObserver queryStatusObserver) {
        QueryOpProcessor rp = createProcessor(output, ops, queryStatusObserver);
        for (QueryOp op : appendops) {
            rp.appendOp(op);
        }
        return rp;
    }

    public String getJob() {
        return job;
    }

    public Query setOps(String ops[]) {
        this.ops = ops;
        return this;
    }

    public String[] getOps() {
        return ops;
    }

    private Query cloneTo(Query q) {
        q.paths = paths;
        q.ops = ops;
        q.job = job;
        q.cachettl = cachettl;
        q.trace = trace;
        q.params = params;
        q.uuid = uuid;
        return q;
    }

    /**
     * @return first a query suitable for the next query worker in the stack
     */
    public Query createPipelinedQuery() {
        Query newQuery = cloneTo(new Query());
        if (ops != null && ops.length > 0) {
            String newops[] = new String[ops.length-1];
            System.arraycopy(ops, 1, newops, 0, newops.length);
            newQuery.ops = newops;
            String pop = ops[0];
            ops = new String[] { pop };
        }
        return newQuery;
    }

    public Query setJob(String job) {
        this.job = job;
        return this;
    }

    public Query setPaths(String path[]) {
        this.paths = path;
        return this;
    }

    public HashMap<String, String> getParameters() {
        return params;
    }

    public Query setParameter(String key, Object value) {
        if (value != null) {
            params.put(key, value.toString());
        } else {
            params.remove(key);
        }
        return this;
    }

    public Query setParameterIfNotYetSet(String key, Object value) {
        if (params.get(key) == null) {
            setParameter(key, value);
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

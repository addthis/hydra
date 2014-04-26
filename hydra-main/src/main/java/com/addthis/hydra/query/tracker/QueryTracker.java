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
package com.addthis.hydra.query.tracker;

import java.io.File;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.text.SimpleDateFormat;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.query.aggregate.MeshSourceAggregator;
import com.addthis.hydra.query.util.HostEntryInfo;
import com.addthis.hydra.util.StringMapHelper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTracker {

     static final Logger log = LoggerFactory.getLogger(QueryTracker.class);

     static final int MAX_FINISHED_CACHE_SIZE = Parameter.intValue("QueryCache.MAX_FINISHED_CACHE_SIZE", 50);
     static final int MAX_CONCURRENT_QUERIES = Parameter.intValue("QueryTracker.MAX_CONCURRENT", 5);
     static final int MAX_QUEUED_QUERIES = Parameter.intValue("QueryTracker.MAX_QUEUED_QUERIES", 100);
     static final int MAX_QUERY_GATE_WAIT_TIME = Parameter.intValue("QueryTracker.MAX_QUERY_GATE_WAIT_TIME", 180);
     static final String logDir = Parameter.value("qmaster.log.dir", "log");
     static final boolean eventLogCompress = Parameter.boolValue("qmaster.eventlog.compress", true);
     static final int logMaxAge = Parameter.intValue("qmaster.log.maxAge", 60 * 60 * 1000);
     static final int logMaxSize = Parameter.intValue("qmaster.log.maxSize", 100 * 1024 * 1024);
     static SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss.SSS");

     final RollingLog eventLog;

    /**
     * Contains the queries that are running
     */
     final ConcurrentMap<String, QueryEntry> running = new ConcurrentHashMap<>();
    /**
     * Contains the queries that are queued because we're getting maxed out
     */
     final ConcurrentMap<String, QueryEntry> queued = new ConcurrentHashMap<>();
     final Cache<String, QueryEntryInfo> recentlyCompleted;
     final Semaphore queryGate = new Semaphore(MAX_CONCURRENT_QUERIES);

    /* metrics */
     final Counter calls = Metrics.newCounter(QueryTracker.class, "queryCalls");
     final Timer queryMeter = Metrics.newTimer(QueryTracker.class, "queryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
     final Counter queryErrors = Metrics.newCounter(QueryTracker.class, "queryErrors");
     final Counter queuedCounter = Metrics.newCounter(QueryTracker.class, "queuedCount");
     final Counter queueTimeoutCounter = Metrics.newCounter(QueryTracker.class, "queueTimeoutCounter");

     final Gauge runningCount = Metrics.newGauge(QueryTracker.class, "RunningCount", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return running.size();
        }
    });

    public QueryTracker() {
        Query.setTraceLog(new RollingLog(new File(logDir, "events-trace"), "queryTrace", eventLogCompress, logMaxSize, logMaxAge));
        this.eventLog = new RollingLog(new File(logDir, "events-query"), "query", eventLogCompress, logMaxSize, logMaxAge);

        this.recentlyCompleted = CacheBuilder.newBuilder()
                .maximumSize(MAX_FINISHED_CACHE_SIZE).build();
        // start timeoutWatcher
        new TimeoutWatcher(this);
    }

    public int getRunningCount() {
        return running.size();
    }

    // --- end mbean ---
    public List<QueryEntryInfo> getRunning() {
        ArrayList<QueryEntryInfo> list = new ArrayList<>(running.size());
        for (QueryEntry e : running.values()) {
            list.add(e.toStat());
        }
        return list;
    }

    public List<QueryEntryInfo> getQueued() {
        ArrayList<QueryEntryInfo> list = new ArrayList<>(queued.size());
        for (QueryEntry e : queued.values()) {
            list.add(e.toStat());
        }
        return list;
    }

    // --- end mbean ---
    public List<HostEntryInfo> getQueryHosts() {
        //for now, adding all host entries
        ArrayList<HostEntryInfo> list = new ArrayList<>();
        for (QueryEntry e : running.values()) {
            list.addAll(getActiveHosts(e.hostInfoSet));
        }
        return list;
    }

    public List<HostEntryInfo> getQueryHosts(String uuid) {
        ArrayList<HostEntryInfo> list = new ArrayList<>();
        QueryEntry queryEntry = running.get(uuid); //first try running
        QueryEntryInfo queryEntryInfo;
        if (queryEntry != null) {
            queryEntryInfo = queryEntry.toStat();
        } else {
            queryEntryInfo = recentlyCompleted.asMap().get(uuid); //if not running, try recentlyCompleted
        }

        if (queryEntryInfo != null) //if not running or recentlycompleted, return empty list
        {
            list.addAll(getActiveHosts(queryEntryInfo.hostInfoSet));
        }
        return list;
    }

    public List<QueryEntryInfo> getCompleted() {
        return new ArrayList<>(recentlyCompleted.asMap().values());
    }

    public boolean cancelRunning(String key, String reason) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        QueryEntry entry = running.get(key);
        if (entry != null) {
            entry.cancel(new QueryException(reason));
        } else {
            log.warn("QT could not get entry from running -- was null : {}", key);
        }

        return entry != null;
    }

    public boolean cancelRunning(String key) {
        return cancelRunning(key, "Canceled by end user");
    }

     void log(StringMapHelper output) {
        if (eventLog == null) {
            log.warn(output.toLog() + "----> EventLog was null redirecting to stdout");
        } else {
            String msg = Strings.cat("<", format.format(new Date()), ".", this.toString(), ">");
            output.add("timestamp", msg);
            eventLog.writeLine(output.createKVPairs().toString());
        }
    }

    public QueryHandle runAndTrackQuery(MeshSourceAggregator meshSourceAggregator,
            Query query, QueryOpProcessor consumer, String[] opsLog) throws QueryException {
        if (queuedCounter.count() > MAX_QUEUED_QUERIES) {
            // server overloaded, reject query to prevent OOM
            throw new QueryException("Unable to handle query: " + query.uuid() + ". Queue size exceeds max value: " + MAX_QUEUED_QUERIES);
        }
        calls.inc();
        QueryEntry entry = new QueryEntry(this, query, consumer, opsLog);

        if (MAX_CONCURRENT_QUERIES > 0 && !acquireQueryGate(query, entry)) {
            throw new QueryException("Timed out waiting for queryGate.  Timeout was: " + MAX_QUERY_GATE_WAIT_TIME + " seconds");
        }
        try {
            log.debug("Executing.... {} {}", query.uuid(), entry.queryDetails);

            // Check if the uuid is repeated, then make a new one
            if (running.putIfAbsent(query.uuid(), entry) != null) {
                String old = query.uuid();
                query.useNextUUID();
                log.warn("Query uuid was already in use in running. Going to try assigning a new one. old:" + old
                         + " new:" + query.uuid());
                if (running.putIfAbsent(query.uuid(), entry) != null) {
                    throw new QueryException("Query uuid was STILL somehow already in use : " + query.uuid());
                }
            }

//            for (QueryData querydata : queryDataCollection) {
//                entry.addHostEntryInfo(querydata.hostEntryInfo);
//            }

            QueryHandle queryHandle = meshSourceAggregator.query(new ResultProxy(entry, consumer));
            entry.queryStart(queryHandle);

            return queryHandle;
        } catch (Exception ex) {
            queryGate.release();
            log.warn("Exception thrown while running query: {}", query.uuid(), ex);
            throw new QueryException(ex);
        }
    }

     boolean acquireQueryGate(Query query, QueryEntry entry) {
        queuedCounter.inc();
        queued.put(query.uuid(), entry);
        boolean acquired = false;
        try {
            if (queryGate.tryAcquire(MAX_QUERY_GATE_WAIT_TIME, TimeUnit.SECONDS)) {
                acquired = true;
            }
        } catch (InterruptedException ignored) {
        }
        if (!acquired) {
            queueTimeoutCounter.inc();
            log.warn("Timed out waiting for queryGate, queryId: {}", query.uuid());
        }
        queued.remove(query.uuid());
        queuedCounter.dec();
        return acquired;
    }

    public static Collection<HostEntryInfo> getActiveHosts(Set<HostEntryInfo> hostInfoSet) {
        List<HostEntryInfo> runningEntries = new ArrayList<>();
        if (hostInfoSet != null) {
            for (HostEntryInfo hostEntryInfo : hostInfoSet) {
                if (hostEntryInfo.getStarttime() > 0) {
                    runningEntries.add(hostEntryInfo);
                }
            }
        }
        return runningEntries;
    }
}

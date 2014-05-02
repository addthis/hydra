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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.text.SimpleDateFormat;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.util.StringMapHelper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr166e.ConcurrentHashMapV8;

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
     final ConcurrentMap<String, QueryEntry> running = new ConcurrentHashMapV8<>();
    /**
     * Contains the queries that are queued because we're getting maxed out
     */
     final ConcurrentMap<String, QueryEntry> queued = new ConcurrentHashMapV8<>();
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

    /**
     * thread pool for query timeout watcher runs. Should only need one thread.
     */
    private final ScheduledExecutorService timeoutWatcherService = new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder().setNameFormat("timeoutWatcher=%d").setDaemon(true).build());

    public QueryTracker() {
        Query.setTraceLog(new RollingLog(new File(logDir, "events-trace"), "queryTrace", eventLogCompress, logMaxSize, logMaxAge));
        this.eventLog = new RollingLog(new File(logDir, "events-query"), "query", eventLogCompress, logMaxSize, logMaxAge);

        this.recentlyCompleted = CacheBuilder.newBuilder()
                .maximumSize(MAX_FINISHED_CACHE_SIZE).build();
        // start timeoutWatcher
        this.timeoutWatcherService.scheduleWithFixedDelay(new TimeoutWatcher(running), 5, 5, TimeUnit.SECONDS);
    }

    public int getRunningCount() {
        return running.size();
    }

    // --- end mbean ---
    public List<QueryEntryInfo> getRunning() {
        ArrayList<QueryEntryInfo> list = new ArrayList<>(running.size());
        for (QueryEntry e : running.values()) {
            list.add(e.getStat());
        }
        return list;
    }

    public List<QueryEntryInfo> getQueued() {
        ArrayList<QueryEntryInfo> list = new ArrayList<>(queued.size());
        for (QueryEntry e : queued.values()) {
            list.add(e.getStat());
        }
        return list;
    }

    // --- end mbean ---
//    public List<HostEntryInfo> getQueryHosts() {
//        //for now, adding all host entries
//        ArrayList<HostEntryInfo> list = new ArrayList<>();
//        for (QueryEntry e : running.values()) {
//            list.addAll(getActiveHosts(e.hostInfoSet));
//        }
//        return list;
//    }

    public QueryEntryInfo getCompletedQueryInfo(String uuid) {
        return recentlyCompleted.asMap().get(uuid);
    }

    public QueryEntry getQueryEntry(String uuid) {
        QueryEntry queryEntry = running.get(uuid); //first try running
        return queryEntry;
    }

    public List<QueryEntryInfo> getCompleted() {
        return new ArrayList<>(recentlyCompleted.asMap().values());
    }

    public boolean cancelRunning(String key) {
        if ((key == null) || key.isEmpty()) {
            return false;
        }
        QueryEntry entry = running.get(key);
        if (entry != null) {
            return entry.cancel();
        } else {
            log.warn("QT could not get entry from running -- was null : {}", key);
            return false;
        }
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

//    public static Collection<HostEntryInfo> getActiveHosts(Set<HostEntryInfo> hostInfoSet) {
//        List<HostEntryInfo> runningEntries = new ArrayList<>();
//        if (hostInfoSet != null) {
//            for (HostEntryInfo hostEntryInfo : hostInfoSet) {
//                if (hostEntryInfo.getStarttime() > 0) {
//                    runningEntries.add(hostEntryInfo);
//                }
//            }
//        }
//        return runningEntries;
//    }
}

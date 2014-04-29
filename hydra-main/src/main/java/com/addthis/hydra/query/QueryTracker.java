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
package com.addthis.hydra.query;

import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.text.SimpleDateFormat;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryConsumer;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.query.util.HostEntryInfo;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.hydra.util.LogUtil;
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

    private static final Logger log = LoggerFactory.getLogger(QueryTracker.class);

    private static final int MAX_FINISHED_CACHE_SIZE = Parameter.intValue("QueryCache.MAX_FINISHED_CACHE_SIZE", 50);
    private static final int MAX_CONCURRENT_QUERIES = Parameter.intValue("QueryTracker.MAX_CONCURRENT", 5);
    private static final int MAX_QUEUED_QUERIES = Parameter.intValue("QueryTracker.MAX_QUEUED_QUERIES", 100);
    private static final int MAX_QUERY_GATE_WAIT_TIME = Parameter.intValue("QueryTracker.MAX_QUERY_GATE_WAIT_TIME", 180);
    private static final String logDir = Parameter.value("qmaster.log.dir", "log");
    private static final boolean eventLogCompress = Parameter.boolValue("qmaster.eventlog.compress", true);
    private static final int logMaxAge = Parameter.intValue("qmaster.log.maxAge", 60 * 60 * 1000);
    private static final int logMaxSize = Parameter.intValue("qmaster.log.maxSize", 100 * 1024 * 1024);

    private final RollingLog eventLog;

    /**
     * Contains the queries that are running
     */
    private final ConcurrentMap<String, QueryEntry> running = new ConcurrentHashMap<>();
    /**
     * Contains the queries that are queued because we're getting maxed out
     */
    private final ConcurrentMap<String, QueryEntry> queued = new ConcurrentHashMap<>();
    private final Cache<String, QueryEntryInfo> recentlyCompleted;
    private final Semaphore queryGate = new Semaphore(MAX_CONCURRENT_QUERIES);

    /* metrics */
    private final Counter calls = Metrics.newCounter(QueryTracker.class, "queryCalls");
    private final Timer queryMeter = Metrics.newTimer(QueryTracker.class, "queryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private final Counter queryErrors = Metrics.newCounter(QueryTracker.class, "queryErrors");
    private final Counter queuedCounter = Metrics.newCounter(QueryTracker.class, "queuedCount");
    private final Counter queueTimeoutCounter = Metrics.newCounter(QueryTracker.class, "queueTimeoutCounter");

    private final Gauge runningCount = Metrics.newGauge(QueryTracker.class, "RunningCount", new Gauge<Integer>() {
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
        new TimeoutWatcher();
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


    public QueryHandle runAndTrackQuery(QuerySource source, Collection<QueryData> queryDataCollection,
            Query query, QueryOpProcessor consumer, String[] opsLog) throws QueryException {
        if (queuedCounter.count() > MAX_QUEUED_QUERIES) {
            // server overloaded, reject query to prevent OOM
            throw new QueryException("Unable to handle query: " + query.uuid() + ". Queue size exceeds max value: " + MAX_QUEUED_QUERIES);
        }
        calls.inc();
        QueryEntry entry = new QueryEntry(query, consumer, opsLog);

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

            for (QueryData querydata : queryDataCollection) {
                entry.addHostEntryInfo(querydata.hostEntryInfo);
            }

            QueryHandle queryHandle = source.query(query, new ResultProxy(entry, consumer));
            entry.queryStart(queryHandle);

            return queryHandle;
        } catch (Exception ex) {
            queryGate.release();
            log.warn("Exception thrown while running query: {}", query.uuid(), ex);
            throw new QueryException(ex);
        }
    }

    private boolean acquireQueryGate(Query query, QueryEntry entry) {
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

    private class QueryEntry {

        private final Query query;
        private final AtomicInteger lines;
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final HashSet<HostEntryInfo> hostInfoSet = new HashSet<>();
        private final int waitTime;
        private final String queryDetails;
        // Stores a reference to the queryopprocessor, which will be used in case the query gets closed
        private final QueryOpProcessor queryOpProcessor;
        private final String[] opsLog;

        private volatile long runTime;
        private volatile long startTime;
        private volatile QueryHandle queryHandle;


        QueryEntry(Query query, QueryOpProcessor queryOpProcessor, String[] opsLog) {
            this.query = query;
            this.queryOpProcessor = queryOpProcessor;
            this.opsLog = opsLog;
            this.lines = new AtomicInteger();
            this.queryDetails = query.getJob() + "--" +
                                (query.getOps() == null ? "" : query.getOps());

            final String timeoutInSeconds = query.getParameter("timeout");
            this.startTime = System.currentTimeMillis();
            if (timeoutInSeconds != null) {
                waitTime = Integer.parseInt(timeoutInSeconds);
            } else {
                waitTime = -1;
            }
        }

        private void queryStart(QueryHandle queryHandle) {
            this.queryHandle = queryHandle;
            this.startTime = System.currentTimeMillis();
        }

        public QueryEntryInfo toStat() {
            QueryEntryInfo stat = new QueryEntryInfo();
            stat.paths = query.getPaths();
            stat.uuid = query.uuid();
            stat.ops = opsLog;
            stat.job = query.getJob();
            stat.alias = query.getParameter("track.alias");
            stat.sources = query.getParameter("sources");
            stat.remoteip = query.getParameter("remoteip");
            stat.sender = query.getParameter("sender");
            stat.lines = lines.get();
            stat.runTime = getRunTime();
            stat.startTime = startTime;
            stat.hostInfoSet = hostInfoSet;
            return stat;
        }

        private long getRunTime() {
            return (runTime > 0) ? runTime : (System.currentTimeMillis() - startTime);
        }

        public boolean addHostEntryInfo(HostEntryInfo newHostEntry) {
            return hostInfoSet.add(newHostEntry);
        }

        @Override
        public String toString() {
            return "QT:" + query.uuid() + ":" + startTime + ":" + runTime + " lines: " + lines;
        }

        /**
         * cancels query if it's still running
         * otherwise, it's a null-op.
         */
        public void cancel(QueryException reason) {
            reason = reason == null ? new QueryException("client cancel") : reason;
            QueryEntry queryEntry = finish(reason);

            if (queryEntry != null && queryEntry.queryHandle != null) {
                queryEntry.queryHandle.cancel(reason.getMessage());
            } else {
                log.warn("Unable to cancel query because queryEntry could be null : {}", queryEntry);
            }
        }

        public QueryEntry finish(DataChannelError error) {
            if (!finished.compareAndSet(false, true)) {
                return null;
            }

            runTime = getRunTime();

            QueryEntry runE = running.remove(query.uuid());
            if (runE == null) {
                log.warn("failed to remove running for {}", query.uuid());
                return null;
            }

            queryGate.release();

            // Finish will be called in case of source error. We need to make sure to close all operators as some
            // might hold non-garbage collectible resources like BDB in gather
            try {
                if (queryOpProcessor != null) {
                    queryOpProcessor.close();
                }
            } catch (Exception e) {
                log.warn("Error while closing queryOpProcessor", e);
            }

            try {
                // If a query never started (generally due to an error with file references) don't record its runtime
                if (startTime > 0) {
                    queryMeter.update(runTime, TimeUnit.MILLISECONDS);
                }

                if (error != null) {
                    LogUtil.log(eventLog, log, new StringMapHelper()
                            .put("type", "query.error")
                            .put("query.path", query.getPaths()[0])
                            .put("query.ops", Arrays.toString(opsLog))
                            .put("sources", query.getParameter("sources"))
                            .put("time", System.currentTimeMillis())
                            .put("time.run", runTime)
                            .put("job.id", query.getJob())
                            .put("job.alias", query.getParameter("track.alias"))
                            .put("query.id", query.uuid())
                            .put("lines", lines)
                            .put("sender", query.getParameter("sender"))
                            .put("error", error.getMessage()));
                    queryErrors.inc();
                    return runE;
                }

                recentlyCompleted.put(query.uuid(), runE.toStat());

                StringMapHelper queryLine = new StringMapHelper()
                        .put("type", "query.done")
                        .put("query.path", query.getPaths()[0])
                        .put("query.ops", Arrays.toString(opsLog))
                        .put("sources", query.getParameter("sources"))
                        .put("time", System.currentTimeMillis())
                        .put("time.run", runTime)
                        .put("job.id", query.getJob())
                        .put("job.alias", query.getParameter("track.alias"))
                        .put("query.id", query.uuid())
                        .put("lines", lines)
                        .put("sender", query.getParameter("sender"));
                LogUtil.log(eventLog, log, queryLine);
            } catch (Exception e) {
                log.warn("Error while doing record keeping for a query.", e);
            }
            return runE;
        }
    }

    /**
     * proxy to watch for end of query to enable state cleanup
     */
    private class ResultProxy implements DataChannelOutput {

        final QueryEntry entry;
        final DataChannelOutput consumer;


        ResultProxy(QueryEntry entry, DataChannelOutput consumer) {
            this.entry = entry;
            this.consumer = new QueryConsumer(consumer);
        }

        @Override
        public void send(Bundle row) {
            consumer.send(row);
            entry.lines.incrementAndGet();
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                entry.lines.addAndGet(bundles.size());
                consumer.send(bundles);
            }
        }

        @Override
        public void sendComplete() {
            try {
                consumer.sendComplete();
                entry.finish(null);
            } catch (Exception ex) {
                sourceError(DataChannelError.promote(ex));
            }
        }

        @Override
        public void sourceError(DataChannelError ex) {
            log.warn("[QueryTracker] sourceError for query: " + entry.query.uuid() + " error: " + ex.getMessage());
            entry.finish(ex);
            consumer.sourceError(ex);
        }

        @Override
        public Bundle createBundle() {
            return consumer.createBundle();
        }
    }

    public static class QueryEntryInfo implements Codec.Codable {

        @Codec.Set(codable = true)
        public String paths[];
        @Codec.Set(codable = true)
        public String uuid;
        @Codec.Set(codable = true)
        public String alias;
        @Codec.Set(codable = true)
        public String sources;
        @Codec.Set(codable = true)
        public String remoteip;
        @Codec.Set(codable = true)
        public String sender;
        @Codec.Set(codable = true)
        public String job;
        @Codec.Set(codable = true)
        public String ops[];
        @Codec.Set(codable = true)
        public long startTime;
        @Codec.Set(codable = true)
        public long runTime;
        @Codec.Set(codable = true)
        public long lines;
        @Codec.Set(codable = true)
        public HashSet<HostEntryInfo> hostInfoSet = new HashSet<>();
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

    private class TimeoutWatcher extends Thread {

        private TimeoutWatcher() {
            setName("QueryTimeoutWatcher");
            setDaemon(true);
            start();
        }

        @Override
        public void run() {
            try {
                long currentTime = System.currentTimeMillis();
                for (QueryEntry queryEntry : running.values()) {
                    if (queryEntry.waitTime <= 0 || queryEntry.query == null) {
                        continue;
                    }
                    long queryDuration = currentTime - queryEntry.startTime;
                    // wait time is in seconds
                    double queryDurationInSeconds = queryDuration / 1000.0;
                    log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " has been running for: " + queryDurationInSeconds + " timeout is: " + queryEntry.waitTime);
                    if (queryDurationInSeconds > queryEntry.waitTime) {
                        log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " has been running for more than " + queryDurationInSeconds + " seconds.  Timeout:" + queryEntry.waitTime + " has been breached.  Canceling query");
                        // sanity check duration
                        if (queryDurationInSeconds > 2 * queryEntry.waitTime) {
                            log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " query duration was insane, resetting to waitTime.  startTime: " + queryEntry.startTime);
                        }
                        sendTimeout(queryEntry, queryEntry.waitTime);
                    }
                }
                Thread.sleep(5000);
            } catch (Exception e) {
                log.warn("[timeout.watcher] exception while watching queries: " + e.getMessage(), e);
            }
        }

        private void sendTimeout(QueryEntry entry, long timeout) {
            String message = "[timeout.watcher] timeout: " + timeout + " has been exceeded, canceling query: " + (entry.query == null ? "" : entry.query.uuid());
            if (entry.queryHandle == null) {
                log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " had a null query handle, removing from running list");
                entry.finish(new DataChannelError(message));

            } else {
                entry.queryHandle.cancel(message);
                if (running.containsKey(entry.query.uuid())) {
                    log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " was still on running list after canceling. Forcing finish");
                    entry.finish(new DataChannelError(message));
                }
            }
        }
    }

}

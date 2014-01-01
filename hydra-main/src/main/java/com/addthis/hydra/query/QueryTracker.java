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
import java.io.IOException;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.text.SimpleDateFormat;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelWriter;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryConsumer;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.query.util.HostEntryInfo;
import com.addthis.hydra.query.util.MeshSourceAggregator;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.hydra.util.StringMapHelper;
import com.addthis.muxy.MuxFile;
import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.MuxFileDirectoryCache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class QueryTracker {

    private static final Logger log = LoggerFactory.getLogger(QueryTracker.class);

    private static final int MAX_FINISHED_CACHE_SIZE = Parameter.intValue("QueryCache.MAX_FINISHED_CACHE_SIZE", 50);
    private static final String QUERY_CACHE_DIR = Parameter.value("QueryCache.CACHE_DIR", "./querycache");
    private static final int QUERY_CACHE_SIZE = Parameter.intValue("QueryCache.CACHE_SIZE", 30000);
    private static final int MAX_CONCURRENT_QUERIES = Parameter.intValue("QueryTracker.MAX_CONCURRENT", 5);
    private static final int MAX_QUEUED_QUERIES = Parameter.intValue("QueryTracker.MAX_QUEUED_QUERIES", 100);
    private static final int MAX_QUERY_GATE_WAIT_TIME = Parameter.intValue("QueryTracker.MAX_QUERY_GATE_WAIT_TIME", 180);
    private static SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss.SSS");
    // 30 minutes
    private static final int DEFAULT_CACHE_TTL = Parameter.intValue("QueryCache.DEFAULT_TTL", (30 * 60 * 1000));

    private final RollingLog eventLog;

    /**
     * Contains the queries that are running
     */
    private final ConcurrentMap<String, QueryEntry> running = new ConcurrentHashMap<>();
    /**
     * Contains the queries that are waiting, because the same exact query is executing and is not cached
     */
    private final ConcurrentMap<String, QueryEntry> waiting = new ConcurrentHashMap<>();
    /**
     * Contains the queries that need to wait before starting to execute
     */
    private final ConcurrentHashMap<Integer, CountDownLatch> othersNeedToWait = new ConcurrentHashMap<>();
    /**
     * Contains the queries that are queued because we're getting maxed out
     */
    private final ConcurrentMap<String, QueryEntry> queued = new ConcurrentHashMap<>();
    private final AtomicBoolean cacheEnabled = new AtomicBoolean(true);
    private final Cache<String, QueryEntryInfo> recentlyCompleted;

    // query cache
    private final MuxFileDirectory multiplexedFileManager;
    private final Map<Integer, CacheData> queryCacheList = Collections.synchronizedMap(new LruCache(QUERY_CACHE_SIZE));
    private final Semaphore queryGate = new Semaphore(MAX_CONCURRENT_QUERIES);
    /* metrics */
    private final Counter calls = Metrics.newCounter(QueryTracker.class, "queryCalls");
    private final Timer queryMeter = Metrics.newTimer(QueryTracker.class, "queryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private final Timer cachedQueryMeter = Metrics.newTimer(QueryTracker.class, "cachedQueryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private final Timer nonCachedQueryMeter = Metrics.newTimer(QueryTracker.class, "nonCachedQueryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private final Counter queryCacheSize = Metrics.newCounter(QueryTracker.class, "queryCacheSize");
    private final Counter queryErrors = Metrics.newCounter(QueryTracker.class, "queryErrors");
    private final Gauge queryCacheListSize;
    private final Gauge queryCacheMuxyEntries;
    private final Counter queuedCounter = Metrics.newCounter(QueryTracker.class, "queuedCount");
    private final Counter queueTimeoutCounter = Metrics.newCounter(QueryTracker.class, "queueTimeoutCounter");
    private final Meter queryCacheHits = Metrics.newMeter(QueryTracker.class, "queryCacheHitMeter", "cacheHit", TimeUnit.SECONDS);
    private final Meter queryCacheMisses = Metrics.newMeter(QueryTracker.class, "queryCacheMisses", "cacheMiss", TimeUnit.SECONDS);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final Gauge runningCount = Metrics.newGauge(QueryTracker.class, "RunningCount", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return running.size();
        }
    });

    public QueryTracker(RollingLog eventLog) throws IOException {
        this.eventLog = eventLog;

        this.recentlyCompleted = CacheBuilder.newBuilder()
                .maximumSize(MAX_FINISHED_CACHE_SIZE).build();
        try {
            // Setup the query cache directory
            File cacheDir = new File(QUERY_CACHE_DIR);
            // Recursively delete the cachedir and all its contents if it exists
            FileUtils.deleteDirectory(cacheDir);
            if (!cacheDir.mkdirs()) {
                throw new RuntimeException("Unable to create cache directory");
            }
            multiplexedFileManager = MuxFileDirectoryCache.getWriteableInstance(cacheDir);
            multiplexedFileManager.setDeleteFreed(true);

            // Returns the actual size of the query cache list
            queryCacheListSize = Metrics.newGauge(QueryTracker.class, "QueryCacheListSize", new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return queryCacheList.size();
                }
            });

            // Returns the number of files stores by muxy
            queryCacheMuxyEntries = Metrics.newGauge(QueryTracker.class, "QueryCacheMuxyEntries", new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return multiplexedFileManager.getFileCount();
                }
            });
        } catch (Exception e) {
            throw new IOException("Unable to open multiplexedFileManager", e);
        }
        // start timeoutWatcher
        new TimeoutWatcher();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                runtimeShutdownHook();
            }
        });
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

    public List<QueryEntryInfo> getWaiting() {
        ArrayList<QueryEntryInfo> list = new ArrayList<QueryEntryInfo>(waiting.size());
        for (QueryEntry e : waiting.values()) {
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
        if (key == null || "".equals(key)) {
            return false;
        }
        QueryEntry entry = running.get(key);
        if (entry != null) {
            entry.cancel(new QueryException(reason));
        } else {
            log.warn("QT could not get entry from running -- was null : " + key);
        }

        return entry != null;
    }

    public boolean cancelRunning(String key) {
        return cancelRunning(key, "Canceled by end user");
    }

    private void log(StringMapHelper output) {
        if (eventLog == null) {
            log.warn(output.toLog() + "----> EventLog was null redirecting to stdout");
        } else {
            String msg = Strings.cat("<", format.format(new java.util.Date()), ".", this.toString(), ">");
            output.add("timestamp", msg);
            eventLog.writeLine(output.createKVPairs().toString());
        }
    }

    public QueryTracker setCacheEnabled(boolean enable) {
        cacheEnabled.set(enable);
        return this;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled.get();
    }

    /**
     * main entry point for this class
     */
    public QuerySource createCacheWrapper(MeshSourceAggregator client, Collection<QueryData> queryDatas) {
        return new Wrapper(client, queryDatas);
    }

    /** */
    private class Wrapper implements QuerySource {

        private QuerySource source;
        private Collection<QueryData> queryDataCollection;

        Wrapper(QuerySource source) {
            this.source = source;
        }

        Wrapper(MeshSourceAggregator source, Collection<QueryData> queryDatas) {
            this.source = source;
            this.queryDataCollection = queryDatas;
        }

        /**
         * Given a QueryEntry, this function will wait as long as there's another entry of equal cacheKey that
         * obtained the latch through the concurrent hash map {@link #othersNeedToWait}. To improve the performance,
         * the function will wait and the
         * {@link #releaseSingleQueryInstanceLock(com.addthis.hydra.query.QueryTracker.QueryEntry)}
         * is expected to notifyAll to wake up all waiting threads.
         *
         * @param entry
         */
        private void acquireSingleQueryInstanceLock(QueryEntry entry) {
            // Wait if another query with the same cache key is currently being checked in this section
            try {
                while (true) {
                    // If possible, avoid creating another latch and leaving it to get GCed, check first if it's null
                    CountDownLatch latch;
                    latch = othersNeedToWait.get(entry.cacheKey);

                    if (latch == null) {
                        latch = new CountDownLatch(1);

                        CountDownLatch oldLatch = othersNeedToWait.putIfAbsent(entry.cacheKey, latch);

                        if (oldLatch == null) {
                            // We put the latch in.... others need to wait
                            return;
                        } else {
                            latch = oldLatch;
                        }
                    }

                    latch.await();
                }
            } catch (InterruptedException e) {
                log.warn("Query: " + entry.query.uuid() + " Interrupted while waiting inside the single instance gate");
            }
        }

        /**
         * Checks if there's another running instance from the same query
         *
         * @param entry a reference to the QueryEntry to be checked
         * @return a String containing the query uuid of the repeated query
         */
        private String checkForRunningInstance(QueryEntry entry) {
            // Check if the same exact query is running and is not cached, if so then we wait
            CacheData cacheData = queryCacheList.get(entry.getCacheKey());

            if (!entry.noCache && cacheData == null) {
                for (Map.Entry<String, QueryEntry> e : running.entrySet()) {
                    if (e.getValue().hashKey == entry.hashKey
                        && e.getValue().queryDetails.equals(entry.queryDetails)) {
                        return e.getKey();
                    }
                }
            }
            return null;
        }

        /**
         * Release the spin lock allowing another instance of the same query to flow in query function.
         *
         * @param entry
         */
        private void releaseSingleQueryInstanceLock(QueryEntry entry) {
            // Remove the entry first, then wake up all waiting threads
            CountDownLatch latch = othersNeedToWait.remove(entry.cacheKey);
            if (latch != null) {
                latch.countDown(); // Wake up everyone else
            }
        }

        /**
         * Will block waiting for the given certain query uuid if it exists in the running list. Blocking will expire
         * if blocked for more than the given {@link QueryEntry#waitTime} in seconds.
         *
         * @param matchingQueryUID a String containing the UUID of the query to wait for
         * @param myQueryEntry     the {@link QueryEntry} of the query requesting to block
         * @throws InterruptedException
         */
        private void waitForQueryToFinish(String matchingQueryUID, QueryEntry myQueryEntry) throws InterruptedException {
            if (log.isDebugEnabled()) {
                log.debug("Waiting.... " + myQueryEntry.query.uuid() + " " + myQueryEntry.queryDetails);
            }

            QueryEntry runningEntry = running.get(matchingQueryUID);

            if (runningEntry != null) { // Check that it is still running
                waiting.putIfAbsent(myQueryEntry.query.uuid(), myQueryEntry);
                runningEntry.waitingLatch.await(myQueryEntry.waitTime, TimeUnit.SECONDS);
                waiting.remove(myQueryEntry.query.uuid());

                if (runningEntry.waitingLatch.getCount() != 0) {
                    // Timed out
                    throw new QueryException("Timed out waiting for a duplicate query " + matchingQueryUID +
                                             " to finish or get cached.  Timeout was: " + myQueryEntry.waitTime + " seconds");
                }
            }
        }

        /**
         * If the {@link #cacheEnabled} is set and the query is cached, this function will return a QueryHandle
         * for a consumer reading off the cache. If the query is not cached, cache is disabled or expired it will
         * return null.
         *
         * @param entry
         * @param consumer
         * @return
         * @throws IOException
         */
        QueryHandle getQueryFromCache(QueryEntry entry, DataChannelOutput consumer) throws IOException {
            CacheData cacheData;
            if (cacheEnabled.get() && !entry.noCache &&
                (cacheData = queryCacheList.get(entry.getCacheKey())) != null &&
                cacheData.queryDetails.equals(entry.queryDetails)) {
                MuxFile fileMeta;
                if (!cacheData.isExpired() && multiplexedFileManager.exists(cacheData.fileName)) {
                    fileMeta = multiplexedFileManager.openFile(cacheData.fileName, false);
                    // query is cached so lets use that
                    QueryCacheConsumer queryCacheConsumer = new QueryCacheConsumer(fileMeta);
                    entry.cacheHit = true;
                    queryCacheHits.mark();

                    return queryCacheConsumer.query(entry.query, new DirectConsumer(entry.query, consumer, entry));
                } else {
                    // bad cache entry, remove it
                    removeCacheEntry(entry.getCacheKey(), cacheData);
                }
            }

            if (cacheEnabled.get() && !entry.noCache) {
                queryCacheMisses.mark();
            }

            return null;
        }

        @Override
        public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException {
            if (queuedCounter.count() > MAX_QUEUED_QUERIES) {
                // server overloaded, reject query to prevent OOM
                throw new QueryException("Unable to handle query: " + query.uuid() + ". Queue size exceeds max value: " + MAX_QUEUED_QUERIES);
            }
            calls.inc();
            boolean gateAcquired = false;
            QueryEntry entry = null;
            try {
                entry = new QueryEntry(query.uuid(), query);
                if (consumer instanceof QueryOpProcessor) {
                    entry.queryOpProcessor = (QueryOpProcessor) consumer;
                    query.queryStatusObserver = query.getQueryStatusObserver();
                }

                if (cacheEnabled.get()) { // Is cache enabled
                    acquireSingleQueryInstanceLock(entry);
                    String matchingQueryUID = checkForRunningInstance(entry);

                    if (matchingQueryUID != null) {
                        releaseSingleQueryInstanceLock(entry);
                        waitForQueryToFinish(matchingQueryUID, entry);
                    }
                }

                if (MAX_CONCURRENT_QUERIES > 0 && !acquireQueryGate(query, entry)) {
                    throw new QueryException("Timed out waiting for queryGate.  Timeout was: " + MAX_QUERY_GATE_WAIT_TIME + " seconds");
                }
                gateAcquired = true;

                if (log.isDebugEnabled()) {
                    log.debug("Executing.... " + query.uuid() + " " + entry.queryDetails);
                }

                // Check if the uuid is repeated, then make a new one
                if (running.putIfAbsent(query.uuid(), entry) != null) {
                    String old = query.uuid();
                    query.useNextUUID();
                    log.warn("Query uuid was already in use in running. Going to try assigning a new one. old:" + old
                             + " new:" + query.uuid());
                    if (running.putIfAbsent(query.uuid(), entry) != null) {
                        releaseSingleQueryInstanceLock(entry);
                        throw new QueryException("Query uuid was STILL somehow already in use : " + query.uuid());
                    }
                }

                // Only release the lock after we have added our instance to the running to make sure we won't
                // end up with two instances in the same time
                releaseSingleQueryInstanceLock(entry);

                QueryHandle cacheConsumer = getQueryFromCache(entry, consumer);
                if (cacheConsumer != null) {
                    return cacheConsumer;
                }

                // not in cache or cache not enabled
                for (QueryData querydata : queryDataCollection) {
                    entry.addHostEntryInfo(querydata.hostEntryInfo);
                }

                return entry.query(source, consumer, cacheEnabled.get());
            } catch (Exception ex) {
                // release gate if needed
                if (gateAcquired) {
                    queryGate.release();
                }
                log.warn("Exception thrown while running query: " + query.uuid(), ex);
                throw new QueryException(ex);
            } finally {
                if (entry != null) {
                    releaseSingleQueryInstanceLock(entry);
                }
            }
        }

        private boolean acquireQueryGate(Query query, QueryEntry entry) throws InterruptedException {
            queuedCounter.inc();
            queued.put(query.uuid(), entry);
            boolean acquired = false;
            if (queryGate.tryAcquire(MAX_QUERY_GATE_WAIT_TIME, TimeUnit.SECONDS)) {
                acquired = true;
            } else {
                queueTimeoutCounter.inc();
                log.warn("Timed out waiting for queryGate, queryId: " + query.uuid());
            }
            queued.remove(query.uuid());
            queuedCounter.dec();
            return acquired;
        }

        @Override
        public void noop() {
            source.noop();
        }

        @Override
        public boolean isClosed() {
            return source.isClosed();
        }
    }

    /** */
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
        public boolean cacheHit;
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


    public Collection<HostEntryInfo> getActiveHosts(Set<HostEntryInfo> hostInfoSet) {
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

    /** */
    private class QueryEntry {

        private String hashKey;
        private final Query query;
        private long runTime;
        private AtomicInteger lines;
        public long startTime;
        private QueryHandle queryHandle;
        private AtomicBoolean finished = new AtomicBoolean(false);
        private QueryException error;
        private HashSet<HostEntryInfo> hostInfoSet = new HashSet<>();
        private final int cacheKey;
        private boolean cacheHit;
        private final long cacheTTL;
        private final int waitTime;
        private final String queryDetails;
        private boolean noCache = false;

        public CountDownLatch waitingLatch = new CountDownLatch(1);

        /**
         * Stores a reference to the queryopprocessor, which will be used in case the query gets closed
         */
        private QueryOpProcessor queryOpProcessor = null;

        QueryEntry(String hashKey, Query query) throws InterruptedException {
            this.hashKey = hashKey;
            this.query = query;
            this.lines = new AtomicInteger();
            this.cacheTTL = query.getParameter("cachettl") == null ? DEFAULT_CACHE_TTL : Long.valueOf(query.getParameter("cachettl"));
            this.noCache = query.getParameter("nocache") == null ? false : Boolean.valueOf(query.getParameter("nocache"));
            this.queryDetails = query.getJob() + "--" +
                                (query.getOps() == null ? "" : query.getOps());

            this.cacheKey = queryDetails.hashCode();
            final String timeoutInSeconds = query.getParameter("timeout");
            if (timeoutInSeconds != null) {
                waitTime = Integer.parseInt(timeoutInSeconds);
            } else {
                waitTime = -1;
            }
        }

        public QueryEntryInfo toStat() {
            QueryEntryInfo stat = new QueryEntryInfo();
            stat.paths = query.getPaths();
            stat.uuid = query.uuid();
            stat.ops = query.getOps();
            stat.job = query.getJob();
            stat.alias = query.getParameter("track.alias");
            stat.sources = query.getParameter("sources");
            stat.remoteip = query.getParameter("remoteip");
            stat.sender = query.getParameter("sender");
            stat.lines = lines.get();
            stat.cacheHit = this.cacheHit;
            stat.runTime = finished.get() ? runTime : (System.currentTimeMillis() - startTime);
            stat.startTime = startTime;
            stat.hostInfoSet = hostInfoSet;
            return stat;
        }

        public boolean addHostEntryInfo(HostEntryInfo newHostEntry) {
            return hostInfoSet.add(newHostEntry);
        }

        @Override
        public String toString() {
            return "QT:" + query.uuid() + ":" + startTime + ":" + runTime + " lines: " + lines + " cacheHit: " + cacheHit;
        }

        public int getCacheKey() {
            return cacheKey;
        }

        /**
         * cancels query if it's still running
         * otherwise, it's a null-op.
         */
        public void cancel(QueryException reason) {
            reason = reason == null ? new QueryException("client cancel") : reason;
            QueryEntry queryEntry = finish(toStat().runTime, toStat().lines, reason);

            if (queryEntry != null && queryEntry.queryHandle != null) {
                queryEntry.queryHandle.cancel(reason.getMessage());
            } else {
                log.warn("Unable to cancel query because queryEntry could be null : " + queryEntry);
            }

        }


        public QueryEntry finish(long time, long lines, DataChannelError error) {
            if (!finished.compareAndSet(false, true)) {
                return null;
            }

            QueryEntry runE = running.remove(query.uuid());
            if (runE == null) {
                if (log.isWarnEnabled()) {
                    log.warn("failed to remove running for " + query.uuid());
                }
                return null;
            }

            runE.waitingLatch.countDown(); // Release all waiting threads after we remove the entry from the running list
            queryGate.release();
            this.lines = new AtomicInteger((int) lines);
            this.runTime = time;

            // Finish will be called in case of source error. We need to make sure to close all operators as some
            // might hold non-garbage collectible resources like BDB in gather
            try {
                if (queryOpProcessor != null) {
                    queryOpProcessor.close();
                    queryOpProcessor = null;
                }
            } catch (IOException e) {
                log.warn("Error while closing queryOpProcessor", e);
                }

            try {
                queryMeter.update(runTime, TimeUnit.MILLISECONDS);

                if (cacheHit) {
                    cachedQueryMeter.update(runTime, TimeUnit.MILLISECONDS);
                } else {
                    nonCachedQueryMeter.update(runTime, TimeUnit.MILLISECONDS);
                }

                if (error != null) {
                    log(new StringMapHelper()
                            .put("type", "query.error")
                            .put("query.path", query.getPaths()[0])
                            .put("query.ops", query.getOps())
                            .put("sources", query.getParameter("sources"))
                            .put("time", System.currentTimeMillis())
                            .put("time.fetch", time)
                            .put("job.id", query.getJob())
                            .put("job.alias", query.getParameter("track.alias"))
                            .put("query.id", query.uuid())
                            .put("lines", lines)
                            .put("sender", query.getParameter("sender"))
                            .put("cache.hit", cacheHit)
                            .put("error", error.getMessage()));
                    queryErrors.inc();
                    return runE;
                }

                recentlyCompleted.put(query.uuid(), runE.toStat());

                StringMapHelper queryLine = new StringMapHelper()
                        .put("type", "query.done")
                        .put("query.path", query.getPaths()[0])
                        .put("query.ops", query.getOps())
                        .put("sources", query.getParameter("sources"))
                        .put("time", System.currentTimeMillis())
                        .put("time.fetch", time)
                        .put("job.id", query.getJob())
                        .put("job.alias", query.getParameter("track.alias"))
                        .put("query.id", query.uuid())
                        .put("lines", lines)
                        .put("sender", query.getParameter("sender"))
                        .put("cache.hit", cacheHit);
                log(queryLine);
            } catch (Exception e) {
                log.warn("Error while doing record keeping for a query.", e);
                }
            return runE;
        }

        public synchronized QueryHandle query(final QuerySource source, final DataChannelOutput consumer, boolean cacheEnabled) throws QueryException {
            try {
                queryHandle = source.query(query, new ResultProxy(this, consumer, cacheEnabled && !noCache));
            } catch (IOException e) {
                throw new QueryException(e);
            }
            return queryHandle;
        }

        public void incrementLines() {
            lines.incrementAndGet();
        }
    }

    /**
     * proxy to watch for end of query to enable state cleanup
     */
    private class ResultProxy implements DataChannelOutput {

        final QueryEntry entry;
        final DataChannelOutput consumer;
        final AtomicLong lines = new AtomicLong(0);
        final long start;
        final AtomicBoolean cacheEnabled = new AtomicBoolean(false);
        final MuxFile cacheData;
        final DataChannelWriter dataChannelWriter;
        final Lock writeLock = new ReentrantLock();
        private Exception cacheException;


        ResultProxy(QueryEntry entry, DataChannelOutput consumer, boolean cacheEnabled) throws IOException {
            this.start = System.currentTimeMillis();
            this.entry = entry;
            this.consumer = new QueryConsumer(consumer);
            entry.startTime = start;
            this.cacheEnabled.set(cacheEnabled);
            if (cacheEnabled) {
                String tempCacheFileName = QUERY_CACHE_DIR + "/" + UUID.randomUUID();
                cacheData = multiplexedFileManager.openFile(tempCacheFileName, true);
                dataChannelWriter = new DataChannelWriter(cacheData.append());
            } else {
                cacheData = null;
                dataChannelWriter = null;
            }
        }

        @Override
        public void send(Bundle row) {
            lines.incrementAndGet();
            consumer.send(row);
            sendCacheData(row);
            entry.incrementLines();
        }

        private void sendCacheData(List<Bundle> bundles) {
            if (cacheEnabled.get() && bundles != null) {
                for (Bundle bundle : bundles) {
                    sendCacheData(bundle);
                }
            }
        }

        private void sendCacheData(Bundle row) {
            if (cacheEnabled.get() && cacheData != null) {
                writeLock.lock();
                try {
                    dataChannelWriter.write(row);
                } catch (IOException e) {
                    cacheEnabled.set(false);
                    cacheException = e;
                } finally {
                    writeLock.unlock();
                }
            }
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                lines.addAndGet(bundles.size());
                consumer.send(bundles);
                sendCacheData(bundles);

            }
        }

        @Override
        public void sendComplete() {
            try {
                consumer.sendComplete();
                if (cacheEnabled.get() && cacheException == null) {
                    dataChannelWriter.close();
                    String fileName = QUERY_CACHE_DIR + "/" + entry.cacheKey;
                    if (log.isDebugEnabled()) {
                        log.debug("Caching query to file: " + fileName);
                    }
                    cacheData.setName(fileName);
                    cacheData.sync();
                    queryCacheSize.inc();

                    // TODO: allow query to override cache ttl time
                    queryCacheList.put(entry.cacheKey, new CacheData(fileName, entry.queryDetails, JitterClock.globalTime(), entry.cacheTTL));
                }
                entry.finish(System.currentTimeMillis() - start, lines.get(), null);
            } catch (Exception ex) {
                sourceError(DataChannelError.promote(ex));
            }
        }

        @Override
        public void sourceError(DataChannelError ex) {
            log.warn("[QueryTracker] sourceError for query: " + entry.query.uuid() + " error: " + ex.getMessage());
            entry.finish(System.currentTimeMillis() - start, lines.get(), ex);
            consumer.sourceError(ex);
        }

        @Override
        public Bundle createBundle() {
            return consumer.createBundle();
        }
    }

    /**
     * called by Thread registered to Runtime triggered by sig-kill
     */
    private void runtimeShutdownHook() {
        try {
            shuttingDown.set(true);
            //This is causing hangs during nice kills. Since we blow up the directory on startup, let's not bother.
            //Probably will need to be handled better for rehydrating cache on startup
//          multiplexedFileManager.waitForWriteClosure();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    /**
     * from stackoverflow:  http://stackoverflow.com/questions/221525/how-would-you-implement-an-lru-cache-in-java-6
     */
    private class LruCache extends LinkedHashMap<Integer, CacheData> {

        private final int maxEntries;

        public LruCache(final int maxEntries) {
            // set access-order to false in the LinkedHashMap. This tells it to evict the oldest inserted
            // without re-ordering just with a mere get.
            super(maxEntries + 1, 1.0f, false);
            this.maxEntries = maxEntries;
        }

        /**
         * Returns <tt>true</tt> if this <code>LruCache</code> has more entries than the maximum specified when it was
         * created.
         * <p/>
         * <p>
         * This method <em>does not</em> modify the underlying <code>Map</code>; it relies on the implementation of
         * <code>LinkedHashMap</code> to do that, but that behavior is documented in the JavaDoc for
         * <code>LinkedHashMap</code>.
         * </p>
         *
         * @param eldest the <code>Entry</code> in question; this implementation doesn't care what it is, since the
         *               implementation is only dependent on the size of the cache
         * @return <tt>true</tt> if the oldest
         * @see java.util.LinkedHashMap#removeEldestEntry(Map.Entry)
         */
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Integer, CacheData> eldest) {
            // Remove the eldest entry if we outgrew the allowed cache size, or if the eldest entry
            // is expired
            if (eldest != null && (super.size() > maxEntries || eldest.getValue().isExpired())) {
                removeCacheEntry(eldest.getKey(), eldest.getValue());
            }
            // we always return false since we do the map manipulation here
            return false;
        }
    }

    private void removeCacheEntry(Integer cacheKey, CacheData cacheData) {
        queryCacheList.remove(cacheKey);
        queryCacheSize.dec();
        // now delete the file
        if (cacheData != null) {
            try {
                MuxFile fileMeta = multiplexedFileManager.openFile(cacheData.fileName, false);
                if (fileMeta != null) {
                    fileMeta.delete();
                } else {
                    log.warn("WARN: cache data was not present on remove");
                }
            } catch (IOException e) {
                log.warn("Exception deleting cache data", e);
                }
        }
    }

    /**
     * watcher wrapper for reporting stats
     */
    private final class DirectConsumer implements DataChannelOutput {

        private Query query;
        private final DataChannelOutput consumer;
        private final AtomicInteger rows = new AtomicInteger(0);
        private final long start = System.currentTimeMillis();
        private final QueryEntry entry;

        DirectConsumer(Query query, DataChannelOutput consumer, QueryEntry queryEntry) {
            this.query = query;
            this.consumer = consumer;
            this.entry = queryEntry;
            this.entry.startTime = start;
        }

        @Override
        public void send(Bundle row) {
            rows.incrementAndGet();
            consumer.send(row);
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                rows.addAndGet(bundles.size());
                consumer.send(bundles);
            }
        }

        @Override
        public void sendComplete() {
            consumer.sendComplete();
            long duration = System.currentTimeMillis() - start;
            entry.finish(duration, rows.get(), null);
        }

        @Override
        public void sourceError(DataChannelError ex) {
            long duration = System.currentTimeMillis() - start;
            consumer.sourceError(ex);
            entry.finish(duration, rows.get(), ex);
        }

        @Override
        public Bundle createBundle() {
            return consumer.createBundle();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("DirectConsumer");
            sb.append("{query=").append(query);
            sb.append(", consumer=").append(consumer);
            sb.append(", rows=").append(rows);
            sb.append(", start=").append(start);
            sb.append('}');
            return sb.toString();
        }
    }

    private class CacheData implements Serializable {

        final String fileName;
        final long createTime;
        final long ttl;
        final String queryDetails;

        private CacheData(String fileName, String queryDetails, long createTime, long ttl) {
            this.fileName = fileName;
            this.createTime = createTime;
            this.ttl = ttl;
            this.queryDetails = queryDetails;
        }

        public boolean isExpired() {
            return (JitterClock.globalTime() - createTime) > ttl;
        }

    }

    private class TimeoutWatcher extends Thread {

        private TimeoutWatcher() {
            setName("QueryTimeoutWatcher");
            setDaemon(true);
            start();
        }

        @Override
        public void run() {
            while (!shuttingDown.get()) {
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
                                queryDurationInSeconds = 0;
                            }
                            sendTimeout(queryEntry, queryEntry.waitTime, (long) queryDurationInSeconds);
                        }
                    }
                    Thread.sleep(5000);
                } catch (Exception e) {
                    log.warn("[timeout.watcher] exception while watching queries: " + e.getMessage(), e);
                    }
            }
        }

        private void sendTimeout(QueryEntry entry, long timeout, long duration) {
            String message = "[timeout.watcher] timeout: " + timeout + " has been exceeded, canceling query: " + (entry.query == null ? "" : entry.query.uuid());
            entry.error = new QueryException(message);
            if (entry.queryHandle == null) {
                log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " had a null query handle, removing from running list");
                entry.finish(duration, entry.lines.get(), new DataChannelError(message));

            } else {
                entry.queryHandle.cancel(message);
                if (running.containsKey(entry.query.uuid())) {
                    log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " was still on running list after canceling. Forcing finish");
                    entry.finish(duration, entry.lines.get(), new DataChannelError(message));
                }
            }
        }
    }

}

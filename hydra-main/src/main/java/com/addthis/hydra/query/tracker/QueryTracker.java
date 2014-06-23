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

import java.io.Closeable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.task.output.TaskDataOutput;
import com.addthis.hydra.util.LogUtil;

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

public class QueryTracker implements Closeable {

    static final Logger log = LoggerFactory.getLogger(QueryTracker.class);

    static final int MAX_FINISHED_CACHE_SIZE = Parameter.intValue("QueryCache.MAX_FINISHED_CACHE_SIZE", 50);

    /**
     * Contains the queries that are running
     */
    final ConcurrentMap<String, QueryEntry> running = new ConcurrentHashMapV8<>();
    final Cache<String, QueryEntryInfo> recentlyCompleted;
    final TaskDataOutput eventLog;

    /* metrics */
    final Counter queryErrors = Metrics.newCounter(QueryTracker.class, "queryErrors");
    final Timer queryMeter = Metrics.newTimer(QueryTracker.class, "queryMeter", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
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
        this.recentlyCompleted = CacheBuilder.newBuilder()
                                             .maximumSize(MAX_FINISHED_CACHE_SIZE)
                                             .build();

        this.timeoutWatcherService.scheduleWithFixedDelay(
                new TimeoutWatcher(running), 5, 5, TimeUnit.SECONDS);
        this.eventLog = LogUtil.newBundleOutputFromConfig("queries");
    }

    @Override
    public void close() {
        eventLog.sendComplete();
    }

    public int getRunningCount() {
        return running.size();
    }

    public List<QueryEntryInfo> getRunning() {
        ArrayList<QueryEntryInfo> list = new ArrayList<>(running.size());
        for (QueryEntry e : running.values()) {
            list.add(e.getStat());
        }
        return list;
    }

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
}

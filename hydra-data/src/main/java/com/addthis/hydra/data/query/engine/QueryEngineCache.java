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
package com.addthis.hydra.data.query.engine;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements an LRU cache to keep our QueryEngines. It is instantiated only from MeshQuerySource.
 * <p/>
 * It uses guava's cache loader to do most of the work. We periodically check to see if new data is available for
 * a job, and if so, asynchronously prepare the new database before swapping it in. Multiple get or refresh attempts
 * will block and wait on the existing one to finish.
 * <p/>
 * As per guava's specs, it is not guaranteed that we will wait until we are at maximum capacity to evict engines.
 * Also, we are okay with evicting non-idle engines, but we do not force them to close. Rather, we set a flag and
 * trust the query using them to close it when it is finished. This means we may have a number of engines open equal
 * to the cache capacity + number of running queries. It is also possible for a few engines to be transiently open
 * while waiting for the eviction listener to close engines. This is somewhat balanced by guava's more aggressive
 * eviction policy, but in general we should not rely on the capacity as being an absolute hard max. In practice, it
 * should be more than sufficient though.
 * <p/>
 * Basic flow is :
 * Constructed from MQSource
 * MQSource calls getAndLease()
 * See if we have a suitable engine
 * If so, return it, if not, make one and return it
 */
public class QueryEngineCache {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineCache.class);

    /**
     * 'soft cap' on the number of engines to have open. this + concurrent queries +/- a few should closely
     * resemble the real cap on open engines
     */
    private static final long DEFAULT_ENGINE_CACHE_SIZE = Parameter.longValue("queryEngineCache.engineCacheSize", 5);

    /**
     * seconds to let an engine be in cache before attempting to refresh it. Refreshing it means checking whether
     * or not the job has finished running and has a new data directory; it does not force the reopening of the same
     * directory. It is important to note that this scheduled refresh is not checked unless a get is called on it,
     * and that even if the refresh returns the old engine, it resets the fail timer.
     */
    private static final long DEFAULT_REFRESH_INTERVAL = Parameter.longValue("queryEngineCache.refreshInterval", 2 * 60);

    /**
     * seconds in between cache malongenance runs. This helps query sources and jobs in lower throughput environments.
     * It does the guava api clean up method which handles any pending expiration events, and also attempts to provoke
     * refresh attempts on cached keys by calling get on them. The latter is more important for our purposes. Without it,
     * relatively idle engines would become stale or subject to undesired eviction by the fail longerval. 0 disables it.
     */
    private static final long DEFAULT_MAINTENANCE_INTERVAL = Parameter.longValue("queryEngineCache.maintenanceInterval", 20 * 60);

    /**
     * seconds to let an engine be in cache after the most recent write. This is longended only for situations
     * where re-opening that engine is failing, and thus while the refresh is not occuring. it might appear that
     * an engine is alive and up to date and this attempts to limit that disparity if desired. Note that by failing,
     * we mean that the refresh method is throwing exceptions.
     */
    private static final long DEFAULT_FAIL_INTERVAL = Parameter.longValue("queryEngineCache.failInterval", 70 * 60);

    /**
     * thread pool for cache maintenance runs. Should only need one thread.
     */
    private final ScheduledExecutorService queryEngineCacheMaintainer = MoreExecutors
            .getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder().setNameFormat("queryEngineCacheMaintainer=%d").build()));

    /**
     * The {@link LoadingCache} that provides the backing data structure for this class.
     * Acts like an intelligent semi-persistent Map that has logic for loading and reloading complex objects.
     */
    protected final LoadingCache<String, QueryEngine> loadingEngineCache;

    private final long engineCacheSize;
    private final long refreshInterval;
    private final long failInterval;
    private final long maintenanceInterval;

    /**
     * Initialize a {@link LoadingCache} that is capable of loading and reloading
     * {@link QueryEngine}s. Reloads occur asynchronously to prevent blocking operations
     * during unrelated calls to the cache.  When reload is called the current engine will be compared with the
     * newest available data directory.  If the current engine is up to date it will be returned, otherwise a new
     * engine will be opened to replace the current engine with the latest available.
     * <p/>
     * On removal, we have a listener that will call closeWhenIdle on engines. It has a guard against removal events
     * generated by refreshes where we decide to keep the existing engine (no new job data is available). There is a
     * race condition where that test can be passed more than once so any clean up done there must be okay with that.
     * The race condition is such that the test will always be passed at least once, and never when the engine is still
     * available to new get calls. This meets our requirements.
     */
    public QueryEngineCache() {
        this(DEFAULT_ENGINE_CACHE_SIZE, DEFAULT_REFRESH_INTERVAL, DEFAULT_FAIL_INTERVAL, DEFAULT_MAINTENANCE_INTERVAL);
    }

    public QueryEngineCache(long engineCacheSize, long refreshInterval, long failInterval, long maintenanceInterval) {
        this(engineCacheSize, refreshInterval, failInterval, maintenanceInterval, new EngineLoader());
    }

    public QueryEngineCache(long engineCacheSize, long refreshInterval, long failInterval, long maintenanceInterval,
            EngineLoader engineLoader) {
        this.engineCacheSize = engineCacheSize;
        this.refreshInterval = refreshInterval;
        this.failInterval = failInterval;
        this.maintenanceInterval = maintenanceInterval;

        log.info("Initializing QueryEngineCache: {}", this); //using 'this' is just more efficient

        // no easy way around escaping 'this' here, but at least it is more obvious what is going on now
        loadingEngineCache = CacheBuilder.newBuilder()
                .maximumSize(engineCacheSize)
                .refreshAfterWrite(refreshInterval, TimeUnit.SECONDS)
                .expireAfterWrite(failInterval, TimeUnit.SECONDS)
                .removalListener(new EngineRemovalListener(this))
                .build(engineLoader);

        //schedule maintenance runs
        maybeInitMaintenance();
    }


    /**
     * schedules maintenance for the cache using the maintenanceInterval parameter. Values less than 1
     * are treated as 'do not do maintenance'. Maintenance includes cache loader cleanUp() and an attempt
     * to trigger refreshes in relatively idle engines. This is done by the thread safe iterator from
     * the loading cache and performing getIfPresent calls on each entry. This will only trigger refreshes
     * if the refresh interval has passed, and avoids a potential race condition where doing refresh() could
     * end up re-loading an engine that was just evicted. This is important because in addition to being
     * incorrect cache behavior, refresh will block instead of being asynchronous while doing so -- possibly
     * leading to even more race conditions.
     * <p/>
     * since the thread safe iterator is weakly consistent, it is a good idea to configure the intervals so
     * that maintenance will be performed more than once before the fail interval occurs (if we do not desire
     * to evict and close 'relatively idle' engines). eg. maintenanceInterval * 2 < failInterval
     * <p/>
     * unfortunately, this somewhat confuses the eviction order heuristic because it considers these all to be
     * valid r/ws. This is one reason to keep this value relatively long. It is possible to optimize against this
     * somewhat, but probably at the cost of greatly increased complexity. It seems unlikely that it will have a
     * large impact if performed infrequently enough though, especially since the evictor is not a simple LRU.
     */
    private void maybeInitMaintenance() {
        if (maintenanceInterval > 0) {
            queryEngineCacheMaintainer.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    loadingEngineCache.cleanUp();
                    for (String key : loadingEngineCache.asMap().keySet()) {
                        loadingEngineCache.getIfPresent(key);
                    }
                }
            }, maintenanceInterval, maintenanceInterval, TimeUnit.SECONDS);
        }
    }

    /**
     * Takes an unresolved (usually the gold path) path to a bdb query directory. This is mostly a thin
     * layer between this class and the backing LoadingCache.
     * <p/>
     * Most importantly, it also attempts to lease the engine. This is because there is a rare race condition
     * where after acquiring the engine, but before leasing it ourselves, it is evicted from the cache. Probably
     * caused by refresh, since it is less likely that an engine we just acquired would be the target of size
     * eviction in most cases. It is relatively unlikely to happen even twice in a row, but we try three times
     * here anyway. I have never seen this exception but if we start to see it a lot, we can re-evaluate this approach.
     *
     * @param directoryPath The path of the engine directory
     * @return a QueryEngine from the cache or constructed on demand (constructing blocks this thread)
     * @throws Exception - any problem while getting the engine. Likely either an issue with leasing or with opening an engine
     */
    public QueryEngine getAndLease(String directoryPath) throws Exception {
        for (int i = 0; i < 3; i++) {
            QueryEngine qe = loadingEngineCache.get(directoryPath);
            if (qe.lease()) {
                return qe;
            }
        }
        log.warn("Tried three times but unable to get lease for engine with path: {}", directoryPath);
        throw new RuntimeException("Can't lease engine");
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("engineCacheSize", engineCacheSize)
                .add("refreshInterval", refreshInterval)
                .add("maintenanceInterval", maintenanceInterval)
                .add("failInterval", failInterval)
                .toString();
    }

}

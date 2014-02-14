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

import java.io.File;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.ReadTree;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

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
    private static final int engineCacheSize = Parameter.intValue("queryEngineCache.engineCacheSize", 5);

    /**
     * seconds to let an engine be in cache before attempting to refresh it. Refreshing it means checking whether
     * or not the job has finished running and has a new data directory; it does not force the reopening of the same
     * directory. It is important to note that this scheduled refresh is not checked unless a get is called on it,
     * and that even if the refresh returns the old engine, it resets the fail timer.
     */
    private static final int refreshInterval = Parameter.intValue("queryEngineCache.refreshInterval", 2 * 60);

    /**
     * seconds in between cache maintenance runs. This helps query sources and jobs in lower throughput environments.
     * It does the guava api clean up method which handles any pending expiration events, and also attempts to provoke
     * refresh attempts on cached keys by calling get on them. The latter is more important for our purposes. Without it,
     * relatively idle engines would become stale or subject to undesired eviction by the fail interval. 0 disables it.
     */
    private static final int maintenanceInterval = Parameter.intValue("queryEngineCache.maintenanceInterval", 20 * 60);

    /**
     * seconds to let an engine be in cache after the most recent write. This is intended only for situations
     * where re-opening that engine is failing, and thus while the refresh is not occuring. it might appear that
     * an engine is alive and up to date and this attempts to limit that disparity if desired. Note that by failing,
     * we mean that the refresh method is throwing exceptions.
     */
    private static final int failInterval = Parameter.intValue("queryEngineCache.failInterval", 70 * 60);

    /**
     * metric to track the number of engines opened. Should be an aggregate of new and refresh'd
     */
    protected static final Counter enginesOpened = Metrics.newCounter(QueryEngineCache.class, "enginesOpened");
    /**
     * metric to track the number of engines refreshed
     */
    protected static final Counter enginesRefreshed = Metrics.newCounter(QueryEngineCache.class, "enginesRefreshed");
    /**
     * metric to track the number of 'new' engines opened
     */
    protected static final Counter newEnginesOpened = Metrics.newCounter(QueryEngineCache.class, "newEnginesOpened");

    /**
     * thread pool for refreshing engines. the max size of the pool tunes how many engine refreshes are allowed to
     * occur concurrently. by limiting this amount, we reduce its ability to interfere with opening new engines.
     * since new engines are actually blocking queries, they are more important.
     */
    private static final int maxRefreshThreads = Parameter.intValue("queryEngineCache.maxRefreshThreads", 1);

    /**
     * try to load all tree nodes from the old engine's cache before completing an engine refresh. Provokes a lot of
     * activity and hopefully helpful cache initialization from the new page cache.
     */
    private static final boolean warmOnRefresh = Parameter.boolValue("queryEngineCache.warmOnRefresh", true);

    private final ExecutorService engineRefresherPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(1, maxRefreshThreads, 5000L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("engineRefresher-%d").build()));

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
    private final LoadingCache<String, QueryEngine> loadingEngineCache;

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
        this(engineCacheSize, refreshInterval, failInterval);
    }

    public QueryEngineCache(int engineCacheSize, int refreshInterval, int failInterval) {
        log.warn("Initializing QueryEngineCache: " + this);
        loadingEngineCache = CacheBuilder.newBuilder()
                .maximumSize(engineCacheSize)
                .refreshAfterWrite(refreshInterval, TimeUnit.SECONDS)
                .expireAfterWrite(failInterval, TimeUnit.SECONDS)
                .removalListener(
                        new RemovalListener<String, QueryEngine>() {
                            public void onRemoval(RemovalNotification<String, QueryEngine> note) {
                                QueryEngine qe = note.getValue();
                                // a refresh call that returns the current value can generate spurious events
                                if (loadingEngineCache.asMap().get(note.getKey()) != qe)
                                {
                                    qe.closeWhenIdle();
                                }
                            }
                        }
                )
                .build(
                        new CacheLoader<String, QueryEngine>() {
                            public QueryEngine load(String dir) throws Exception {
                                QueryEngine qe = createEngine(dir);
                                newEnginesOpened.inc();
                                return qe;
                            }

                            @Override
                            public ListenableFuture<QueryEngine> reload(final String dir, final QueryEngine oldValue) throws Exception {
                                if (((QueryEngineDirectory) oldValue).isOlder(dir)) //test for new data
                                {
                                    ListenableFutureTask<QueryEngine> task = ListenableFutureTask.create(new Callable<QueryEngine>() {
                                        @Override
                                        public QueryEngine call() throws Exception {
                                            QueryEngine qe = createEngine(dir);
                                            if (warmOnRefresh) {
                                                try {
                                                    ((QueryEngineDirectory) qe).loadAllFrom((QueryEngineDirectory) oldValue);
                                                } catch (Exception e) //should either swallow if recoverable or close and rethrow if not
                                                {
                                                    e.printStackTrace(); //can't think of any reason it wouldn't be recoverable
                                                }
                                            }
                                            enginesRefreshed.inc();
                                            return qe;
                                        }
                                    });
                                    engineRefresherPool.submit(task);
                                    return task;
                                } else {
                                    SettableFuture<QueryEngine> task = SettableFuture.create();
                                    task.set(oldValue);
                                    return task;
                                }
                            }
                        });
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
        log.warn("Tried three times but unable to get lease for engine with path: " + directoryPath);
        throw new RuntimeException("Can't lease engine");
    }

    /**
     * Creates engines for the cacheLoader and is called by load and reload to be the backend of get/refresh.
     * Takes an unresolved directory, resolves it, creates an engine with the resolved directory, and returns it.
     * If for any reason, it fails to create an engine, it throws an exception and attempts to close the tree/bdb if needed
     *
     * @param dir - Directory to open engine for -- may be (and usually is) an unresolved path with symlinks
     * @return QueryEngine for that directory
     * @throws Exception - any problem while making the engine
     */
    protected QueryEngine createEngine(String dir) throws Exception {
        String canonicalDirString = new File(dir).getCanonicalPath();

        DataTree tree = new ReadTree(new File(canonicalDirString));
        enginesOpened.inc(); //Metric for total trees/engines initialized
        try {
            return new QueryEngineDirectory(tree, canonicalDirString);
        } catch (Exception e) {
            tree.close();
            throw e;
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("engineCacheSize", engineCacheSize)
                .add("refreshInterval", refreshInterval)
                .add("maintenanceInterval", maintenanceInterval)
                .add("failInterval", failInterval)
                .add("maxRefreshThreads", maxRefreshThreads)
                .add("warmOnRefresh", warmOnRefresh)
                .add("enginesOpened", enginesOpened.count())
                .add("enginesRefreshed", enginesRefreshed.count())
                .add("newEnginesOpened", newEnginesOpened.count())
                .add("engineRefresherPool", engineRefresherPool)
                .add("queryEngineCacheMaintainer", queryEngineCacheMaintainer)
                .add("loadingEngineCache", loadingEngineCache)
                .toString();
    }
}

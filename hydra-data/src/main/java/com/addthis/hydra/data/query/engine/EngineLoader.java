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

import java.io.File;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.ReadTree;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EngineLoader extends CacheLoader<String, QueryEngine> {

    private static final Logger log = LoggerFactory.getLogger(EngineLoader.class);

    /**
     * thread pool for refreshing engines. the max size of the pool tunes how many engine refreshes are allowed to
     * occur concurrently. by limiting this amount, we reduce its ability to interfere with opening new engines.
     * since new engines are actually blocking queries, they are more important.
     */
    private static final int MAX_REFRESH_THREADS = Parameter.intValue("queryEngineCache.maxRefreshThreads", 1);

    /**
     * metric to track the number of 'new' engines opened
     */
    protected static final Meter newEnginesOpened = Metrics.newMeter(QueryEngineCache.class, "newEnginesOpened",
            "newEnginesOpened", TimeUnit.MINUTES);

    private final ExecutorService engineRefresherPool =
            new ThreadPoolExecutor(1, MAX_REFRESH_THREADS, 5000L, TimeUnit.MILLISECONDS,
                                   new LinkedBlockingQueue<>(),
                                   new ThreadFactoryBuilder().setDaemon(true)
                                                             .setNameFormat("engineRefresher-%d")
                                                             .build());

    @Override
    public QueryEngine load(String dir) throws Exception {
        QueryEngine qe = newQueryEngineDirectory(dir);
        newEnginesOpened.mark();
        return qe;
    }

    @Override
    public ListenableFuture<QueryEngine> reload(final String dir, final QueryEngine oldValue) throws Exception {
        //test for new data
        if (((QueryEngineDirectory) oldValue).isOlder(dir)) {
            ListenableFutureTask<QueryEngine> task =
                    ListenableFutureTask.create(new RefreshEngineCall(dir, oldValue, this));
            engineRefresherPool.submit(task);
            return task;
        } else {
            SettableFuture<QueryEngine> task = SettableFuture.create();
            task.set(oldValue);
            return task;
        }
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
    protected QueryEngine newQueryEngineDirectory(String dir) throws Exception {
        String canonicalDirString = new File(dir).getCanonicalPath();

        DataTree tree = new ReadTree(new File(canonicalDirString));
        try {
            return new QueryEngineDirectory(tree, canonicalDirString);
        } catch (Exception e) {
            tree.close();
            throw e;
        }
    }
}

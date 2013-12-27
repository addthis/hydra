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
package com.addthis.hydra.job.store;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * A cache implementation that never blocks unless there is no data for a given ID. Stale values are refreshed asynchronously
 * and the old value is returned in the mean time.
 *
 * @param <T> The class that will be stored in the cache
 */
public abstract class AvailableCache<T> {

    /* A LoadingCache used to save fetched objects */
    private final LoadingCache<String, Optional<T>> loadingCache;

    /* An Executor that will run the background updates to the loadingCache */
    private final ExecutorService executor;

    /**
     * Make a cache using specified cache parameters
     *
     * @param refreshMillis How frequently values should be refreshed in milliseconds (if <= 0, no refresh)
     * @param expireMillis  How old values should have to be before they are expired (if <= 0, they never expire)
     * @param maxSize       How many values should be stored in the cache (if <= 0, no explicit limit)
     * @param fetchThreads  How many threads to use to fetch values in the background (if <=0, use two threads)
     */
    public AvailableCache(long refreshMillis, long expireMillis, int maxSize, int fetchThreads) {
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        // Configure the cache for any parameters that are > 0
        if (expireMillis > 0) {
            cacheBuilder.expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS);
        }
        if (refreshMillis > 0) {
            cacheBuilder.refreshAfterWrite(refreshMillis, TimeUnit.MILLISECONDS);
        }
        if (maxSize > 0) {
            cacheBuilder.maximumSize(maxSize);
        }
        fetchThreads = fetchThreads > 0 ? fetchThreads : 2;
        executor = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(fetchThreads, fetchThreads, 1000L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
        //noinspection unchecked
        this.loadingCache = cacheBuilder.build(new CacheLoader<String, Optional<T>>() {
            @Override
            /**
             * If refreshAfterWrite is enabled, this method is called after returning the old value.
             * The new value will be inserted into the cache when the load() operation completes.
             */
            public ListenableFuture<Optional<T>> reload(final String key, Optional<T> oldval) {
                ListenableFutureTask<Optional<T>> task = ListenableFutureTask.create(new Callable<Optional<T>>() {
                    public Optional<T> call() throws Exception {
                        return load(key);
                    }
                });
                executor.execute(task);
                return task;
            }

            @Override
            public Optional<T> load(String id) throws Exception {
                return Optional.fromNullable(fetchValue(id));
            }
        });
    }

    /**
     * A possibly-lengthy operation to fetch the canonical value for a given id, such as by reading from a SpawnDataStore
     *
     * @param id The id to fetch
     * @return A possibly-null object to put into the cache
     */
    public abstract T fetchValue(String id);


    public T get(String id) throws ExecutionException {
        return loadingCache.get(id).orNull();
    }

    public void remove(String id) {
        loadingCache.invalidate(id);
    }

    public void put(String id, T value) {
        if (id == null || value == null) {
            return;
        }
        loadingCache.put(id, Optional.fromNullable(value));
    }

    public void clear() {
        loadingCache.invalidateAll();
    }

}

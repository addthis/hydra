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

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;

/**
 * A cache implementation that never blocks unless there is no data for a given ID. Stale values are refreshed asynchronously
 * and the old value is returned in the mean time.
 *
 * @param <T> The class that will be stored in the cache
 */
public abstract class AvailableCache<T> implements AutoCloseable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(AvailableCache.class);

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
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
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
        if (fetchThreads <= 0) {
            fetchThreads = 2;
        }
        cacheBuilder.removalListener(new RemovalListener<Optional<T>, Optional<T>>() {
            @Override
            public void onRemoval(RemovalNotification<Optional<T>, Optional<T>> notification) {
                log.info("alias {} and its job {} removed from cache", notification.getKey(), notification.getValue());
            }
        });
        executor = new ThreadPoolExecutor(
                fetchThreads, fetchThreads, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("avail-cache-%d").setDaemon(true).build());
        //noinspection unchecked

        this.loadingCache = cacheBuilder.build(new CacheLoader<String, Optional<T>>() {
            /**
             * If refreshAfterWrite is enabled, this method is called after returning the old value.
             * The new value will be inserted into the cache when the load() operation completes.
             */
            @Override
            public ListenableFuture<Optional<T>> reload(final String key, Optional<T> oldValue) throws Exception {
                ListenableFutureTask<Optional<T>> task = ListenableFutureTask.create(() -> load(key));
                executor.execute(task);
                return task;
            }

            @Override
            public Optional<T> load(String key) throws Exception {
                return Optional.fromNullable(fetchValue(key));
            }
        });
    }

    public LoadingCache<String, Optional<T>> getLoadingCache() {
        return loadingCache;
    }

    /**
     * A possibly-lengthy operation to fetch the canonical value for a given id, such as by reading from a SpawnDataStore
     *
     * @param id The id to fetch
     * @return A possibly-null object to put into the cache
     */
    @Nullable public abstract T fetchValue(String id);


    @Nullable public T get(String id) throws ExecutionException {
        return loadingCache.get(id).orNull();
    }

    public void remove(String id) {
        loadingCache.invalidate(id);
    }

    public void put(String id, T value) {
        if ((id == null) || (value == null)) {
            return;
        }
        loadingCache.put(id, Optional.of(value));
    }

    public void clear() {
        loadingCache.invalidateAll();
    }

    public void cleanUp() {
        loadingCache.cleanUp();
    }

    @Override public void close() throws Exception {
        MoreExecutors.shutdownAndAwaitTermination(executor, 120, TimeUnit.SECONDS);
    }
}

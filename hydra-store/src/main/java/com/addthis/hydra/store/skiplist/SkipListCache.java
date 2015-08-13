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
package com.addthis.hydra.store.skiplist;

import com.addthis.basis.util.Parameter;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.common.AbstractPage;
import com.addthis.hydra.store.common.AbstractPageCache;
import com.addthis.hydra.store.common.ExternalMode;
import com.addthis.hydra.store.common.Page;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.util.MetricsUtil;
import com.addthis.hydra.store.util.NamedThreadFactory;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ::TWO INVARIANTS TO AVOID DEADLOCK AND MAINTAIN CONSISTENCY::
 * <p/>
 * Invariant #1:
 * When locking two pages always lock the lower page before locking the higher page.
 * <p/>
 * Invariant #2:
 * To read a consistent snapshot of a page in the external storage you must
 * be holding a lock on the lower page in memory.
 * <p/>
 * The left sentinel page is the lowest page in storage. It is constructed with
 * a special first key with value negative infinity. No key may be smaller than
 * negative infinity. The left sentinel page may be neither <i>purged</i>
 * nor <i>deleted</i> (see below).
 * <p/>
 * A page is <i>evicted</i> when the contents of the page are transferred from
 * the JVM heap into the external storage. When a page is evicted a page stub
 * remains in memory that contains the minimal information needed to restore the
 * page into memory.
 * <p/>
 * A page is <i>purged</i> when a page stub is deleted from memory. The most
 * recent copy of this page still resides in the external storage. The left
 * sentinel page may not be purged.
 * <p/>
 * A page is <i>deleted</i> when it is removed from both memory and the external storage.
 * Only pages with 0 keys may be deleted. The left sentinel page may not be deleted.
 *
 * @param <K>
 * @param <V>
 */
public class SkipListCache<K, V extends BytesCodable> extends AbstractPageCache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(SkipListCache.class);

    private static final int defaultEvictionThreads = Parameter.intValue("cache.threadcount.eviction", 1);

    /**
     * Used as an absolute delta from maxPages when using that upper bound.
     * Otherwise it's treated as a percentage of maxTotalMemory.
     */
    private static final int shouldEvictDelta = Parameter.intValue("eps.cache.evict.delta", 20);



    final BlockingQueue<Page<K, V>> evictionQueue;

    final ConcurrentSkipListSet<K> purgeSet;

    final AtomicInteger purgeSetSize = new AtomicInteger(0);

    /**
     * Used to schedule synchronous page eviction in the
     * {@link #put(Object, BytesCodable)} and {@link #remove(Object)}
     * methods when the background eviction threads are behind schedule.
     */
    private final LinkedBlockingQueue<BackgroundEvictionTask> evictionTaskQueue;

    private final ScheduledExecutorService evictionThreadPool, purgeThreadPool;


    private static final int evictionThreadSleepMillis = 10;
    private static final int threadPoolWaitShutdownSeconds = 10;

    /**
     * The Builder pattern allows many different variations of a class to
     * be instantiated without the pitfalls of complex constructors. See
     * ''Effective Java, Second Edition.'' Item 2 - "Consider a builder when
     * faced with many constructor parameters."
     */
    public static class Builder<K, V extends BytesCodable> {

        // Required parameters
        protected final int maxPageSize;
        protected final ByteStore externalStore;
        protected final KeyCoder<K, V> keyCoder;

        // Optional parameters - initialized to default values;
        protected int numEvictionThreads = defaultEvictionThreads;
        protected int maxPages = defaultMaxPages;
        @SuppressWarnings("unchecked")
        protected PageFactory<K, V> pageFactory = ConcurrentPage.ConcurrentPageFactory.singleton;

        public Builder(KeyCoder<K, V> keyCoder, ByteStore store, int maxPageSize) {
            this.externalStore = store;
            this.maxPageSize = maxPageSize;
            this.keyCoder = keyCoder;
        }

        @SuppressWarnings("unused")
        public Builder<K, V> numEvictionThreads(int val) {
            numEvictionThreads = val;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<K, V> maxPages(int val) {
            maxPages = val;
            return this;
        }
        @SuppressWarnings("unused")
        public Builder<K, V> pageFactory(PageFactory<K, V> factory) {
            pageFactory = factory;
            return this;
        }

        public SkipListCache<K, V> build() {
            return new SkipListCache<>(keyCoder, externalStore, maxPageSize,
                    maxPages, numEvictionThreads, pageFactory);
        }

    }


    public SkipListCache(KeyCoder<K, V> keyCoder, ByteStore externalStore, int maxPageSize,
                         int maxPages, int numEvictionThreads, PageFactory<K, V> pageFactory) {
        super(keyCoder, externalStore, pageFactory, maxPageSize, maxPages, true);

        this.evictionTaskQueue = new LinkedBlockingQueue<>();
        this.purgeSet = new ConcurrentSkipListSet<>();

        this.evictionQueue = new LinkedBlockingQueue<>();

        evictionThreadPool = Executors.newScheduledThreadPool(numEvictionThreads,
                new NamedThreadFactory(scope + "-eviction-", true));

        purgeThreadPool = Executors.newScheduledThreadPool(numEvictionThreads,
                new NamedThreadFactory(scope + "-purge-", true));

        for (int i = 0; i < numEvictionThreads; i++) {
            purgeThreadPool.scheduleAtFixedRate(new BackgroundPurgeTask(),
                    i,
                    evictionThreadSleepMillis,
                    TimeUnit.MILLISECONDS);

            evictionThreadPool.scheduleAtFixedRate(new BackgroundEvictionTask(0),
                    i,
                    evictionThreadSleepMillis,
                    TimeUnit.MILLISECONDS);
        }

        log.info("[init] ro=" + isReadOnly() + " maxPageSize=" + maxPageSize +
                " maxPages=" + maxPages + " gztype=" + AbstractPage.gztype + " gzlevel=" +
                AbstractPage.gzlevel + " gzbuf=" + AbstractPage.gzbuf + " mem[page=" + mem_page + " type=SkipListCache]");

    }


    class BackgroundPurgeTask implements Runnable {

        Iterator<K> targetKeys;

        BackgroundPurgeTask() {
            targetKeys = purgeSet.iterator();
        }

        @Override
        public void run() {
            try {
                backgroundPurge();
            } catch (Exception ex) {
                logException("Uncaught exception in skiplist concurrent cache purge thread", ex);
            }
        }

        private void backgroundPurge() {
            while (!shutdownEvictionThreads.get() && shouldPurgePage() && doPurgePage()) ;
        }

        /**
         * Return true if-and-only if no further processing is necessary.
         */
        private EvictionStatus removePageFromCache(K targetKey) {
            assert (!targetKey.equals(negInf));

            Page<K, V> prevPage = null, currentPage = null;

            try {

                // We must acquire the locks on the pages from lowest to highest.
                // This is inefficient but it avoids deadlock.
                Map.Entry<K, Page<K, V>> prevEntry, currentEntry;
                prevEntry = getCache().lowerEntry(targetKey);
                prevPage = prevEntry.getValue();
                if (!prevPage.writeTryLock()) {
                    prevPage = null;
                    return EvictionStatus.TRYLOCK_FAIL;
                }
                if (prevPage.inTransientState()) {
                    return EvictionStatus.TRANSIENT_PAGE;
                }

                currentEntry = getCache().higherEntry(prevEntry.getKey());
                if (currentEntry != null) {
                    currentPage = currentEntry.getValue();
                    if (!currentPage.writeTryLock()) {
                        currentPage = null;
                        return EvictionStatus.TRYLOCK_FAIL;
                    }
                    int compareKeys = compareKeys(targetKey, currentPage.getFirstKey());
                    if (compareKeys < 0) {
                        return EvictionStatus.NO_STATUS;
                    } else if (compareKeys == 0 && currentPage.keys() == null &&
                            currentPage.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
                        currentPage.setState(ExternalMode.MEMORY_EVICTED);
                        getCache().remove(targetKey);
                        cacheSize.getAndDecrement();
                        return EvictionStatus.SUCCESS;
                    }
                }
                return EvictionStatus.EVICTED_PAGE;
            } finally {
                writeUnlockAndNull(currentPage);
                writeUnlockAndNull(prevPage);
            }
        }

        /**
         * Returns <code>true</code> is a page is purged and
         * false otherwise.
         */
        private boolean doPurgePage() {
            if (targetKeys == null) {
                targetKeys = purgeSet.iterator();
            }

            while (targetKeys.hasNext()) {
                K minKey = targetKeys.next();
                EvictionStatus status = removePageFromCache(minKey);
                if (status.removePurgeSet()) {
                    if (purgeSet.remove(minKey)) {
                        purgeSetSize.getAndDecrement();
                        return true;
                    }
                }
            }
            targetKeys = null;
            return false;
        }

    }

    public boolean shouldPurgePage() {
        return purgeSetSize.get() > getNumPagesInMemory();
    }



    protected V doPut(K key, V value) {
        V prev;

        /**
         * If the background eviction threads are behind schedule,
         * then synchronously perform a page eviction. The
         * {@link #getEvictionTask()} and {@link #putEvictionTask(BackgroundEvictionTask)}
         * method are for re-using BackgroundEvictionTask object.
         */
        if (mustEvictPage()) {
            BackgroundEvictionTask task = getEvictionTask();
            task.run();
            putEvictionTask(task);
        }

        Page<K, V> page = locatePage(key, LockMode.WRITEMODE);

        try {
            prev = putIntoPage(page, key, value);

            int prevMem = page.getMemoryEstimate();
            page.updateMemoryEstimate();
            updateMemoryEstimate(page.getMemoryEstimate() - prevMem);

            if (page.splitCondition()) {
                splitPage(page);
            } else if (page.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
                page.setState(ExternalMode.DISK_MEMORY_DIRTY);
            }
        } finally {
            page.writeUnlock();
        }

        return prev;
    }

    protected void doRemove(K start, K end) {
        while (true) {
            if (mustEvictPage()) {
                BackgroundEvictionTask task = getEvictionTask();
                task.run();
                putEvictionTask(task);
            }

            Page<K, V> page = locatePage(start, LockMode.WRITEMODE);
            try {
                int startOffset = binarySearch(page.keys(), start, comparator);
                int endOffset = binarySearch(page.keys(), end, comparator);
                int pageSize = page.size();

                if (startOffset < 0) {
                    startOffset = ~startOffset;
                }

                if (endOffset < 0) {
                    endOffset = ~endOffset;
                }


                if (startOffset < endOffset) {
                    int memEstimate = page.getMemoryEstimate();
                    int length = (endOffset - startOffset);
                    for (int i = 0; i < length; i++) {
                        page.keys().remove(startOffset);
                        page.values().remove(startOffset);
                        page.rawValues().remove(startOffset);
                    }
                    page.setSize(page.size() - length);

                    if (page.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
                        page.setState(ExternalMode.DISK_MEMORY_DIRTY);
                    }

                    page.updateMemoryEstimate();
                    updateMemoryEstimate(page.getMemoryEstimate() - memEstimate);
                }

                if (page.size() == 0 && !page.getFirstKey().equals(negInf)) {
                    K targetKey = page.getFirstKey();
                    page = writeUnlockAndNull(page);
                    deletePage(targetKey);
                    continue;
                } else if (endOffset == pageSize) {
                    byte[] higherKeyEncoded = externalStore.higherKey(keyCoder.keyEncode(page.getFirstKey()));
                    if (higherKeyEncoded != null) {
                        start = keyCoder.keyDecode(higherKeyEncoded);
                        continue;
                    }
                }
            } finally {
                writeUnlockAndNull(page);
            }

            break;
        }
    }

    protected V doRemove(K key) {
        if (mustEvictPage()) {
            BackgroundEvictionTask task = getEvictionTask();
            task.run();
            putEvictionTask(task);
        }

        Page<K, V> page = locatePage(key, LockMode.WRITEMODE);
        try {
            if (page.size() == 0) {
                if (!page.getFirstKey().equals(negInf)) {
                    K targetKey = page.getFirstKey();
                    page = writeUnlockAndNull(page);
                    deletePage(targetKey);
                }

                return null;
            }
            int offset = binarySearch(page.keys(), key, comparator);

            // An existing (key, value) pair is found.
            if (offset >= 0) {
                int memEstimate = page.getMemoryEstimate();

                page.fetchValue(offset);

                page.keys().remove(offset);
                page.rawValues().remove(offset);
                V prev = page.values().remove(offset);

                page.setSize(page.size() - 1);

                if (page.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
                    page.setState(ExternalMode.DISK_MEMORY_DIRTY);
                }

                page.updateMemoryEstimate();
                updateMemoryEstimate(page.getMemoryEstimate() - memEstimate);

                if (page.size() == 0 && !page.getFirstKey().equals(negInf)) {
                    K targetKey = page.getFirstKey();
                    page = writeUnlockAndNull(page);
                    deletePage(targetKey);
                }

                return prev;
            } else {
                return null;
            }
        } finally {
            writeUnlockAndNull(page);
        }
    }

    /**
     * Helper method for loadPage().
     */
    private Page<K, V> loadPageCacheFloorEntry(Page<K, V> current, K externalKey) {
        boolean useHint = false;
        try {
            while (true) {
                Map.Entry<K, Page<K, V>> cacheEntry = getCache().floorEntry(externalKey);
                K cacheKey = cacheEntry.getKey();
                Page<K, V> cachePage = cacheEntry.getValue();

                assert (cacheKey.equals(cachePage.getFirstKey()));

                /** If the nearest page in cache equals the new page then return. */
                /** If we did not provide a hint then begin with the nearest page in cache. */
                /** If we provided a hint and it was incorrect then do not use the hint. */
                if (cacheKey.equals(externalKey) || current == null || !cacheKey.equals(current.getFirstKey())) {
                    current = writeUnlockAndNull(current);
                    cachePage.writeLock();
                    if (cachePage.inTransientState()) {
                        cachePage.writeUnlock();
                        continue;
                    } else {
                        return cachePage;
                    }
                }
                /** Else we are using the hint that was provided. */
                else {
                    useHint = true;
                    return current;
                }
            }
        } finally {
            if (!useHint) {
                writeUnlockAndNull(current);
            }
        }
    }


    /**
     * This method loads a page from the external storage if that page is not
     * found in the memory cache. The page that is loaded has the floor key
     * (greatest key less than or equal to) of the input key. Current can
     * be used as a hint to locate the new page. If current is non-null then
     * it must be write-locked.
     * <p/>
     * The target page is returned and it is either read-locked or write-locked
     * depending on the {@param mode} parameter. It is guaranteed that the
     * return page is not in a transient state. It is not guaranteed that the
     * return page has been loaded into memory, ie. (page.keys != null).
     */
    protected Page<K, V> loadPage(K key, Page<K, V> current) {
        assert (current == null || current.isWriteLockedByCurrentThread());

        Page<K, V> next = null, cachePage;

        try {
            byte[] encodedKey = keyCoder.keyEncode(key);

            while (true) {
                byte[] externalKeyEncoded = externalStore.floorKey(encodedKey);

                /** Key of the page that will be loaded from disk. */
                K externalKey = keyCoder.keyDecode(externalKeyEncoded);

                if ((current != null) && (compareKeys(current.getFirstKey(), externalKey) >= 0)) {
                    String errorMessage =
                            "[CORRUPT PAGE STORE] current page key is greater or equal than the one being pulled from" +
                                    " disk (%s >= %s). This is unexpected and likely to lead to an infinite loop if allowed.";
                    throw new AssertionError(String.format(errorMessage, current.getFirstKey(), externalKey));
                }
                assert (compareKeys(key, externalKey) >= 0);

                // Transfer ownership of the 'current' variable to the inner method
                // to handle failures.
                Page<K, V> currentCopy = current;
                current = null;

                /** Locate the nearest page in cache that less than or equal to the new page. */
                cachePage = loadPageCacheFloorEntry(currentCopy, externalKey);

                if (cachePage.getFirstKey().equals(externalKey)) {
                    cachePage.setTimeStamp(generateTimestamp());
                    return cachePage;
                } else {
                    current = cachePage;
                }

                assert (!current.inTransientState());

                findnext:
                while (true) {
                    do {
                        writeUnlockAndNull(next);
                        Map.Entry<K, Page<K, V>> higherEntry = getCache().higherEntry(current.getFirstKey());
                        if (higherEntry == null) {
                            break findnext;
                        }
                        next = higherEntry.getValue();
                        next.writeLock();
                    }
                    while (next.inTransientState());

                    if (compareKeys(next.getFirstKey(), externalKey) >= 0) {
                        break;
                    }
                    current.writeUnlock();
                    current = next;
                    next = null;
                }

                if (next != null && next.getFirstKey().equals(externalKey)) {
                    current = writeUnlockAndNull(current);
                    cachePage = next;
                    next = null;
                    cachePage.setTimeStamp(generateTimestamp());
                    return cachePage;
                }

                byte[] floorPageEncoded = externalStore.get(externalKeyEncoded);

                if (floorPageEncoded == null) {
                    current = writeUnlockAndNull(current);
                    next = writeUnlockAndNull(next);
                    continue;
                }

                return constructNewPage(current, next, externalKey, floorPageEncoded, true);
            }
        } finally {
            writeUnlockAndNull(current);
            writeUnlockAndNull(next);
        }
    }

    @Override
    public K getLastKey() {
        // Fast path: the last page in cache happens to be the last page on disk.
        K fastPath = lastKeyFastPath();
        if (fastPath != null) return fastPath;

        K currentKey;
        byte[] currentKeyEncoded;
        Page<K, V> currentPage = null, prevPage = null;

        // Slow path: we load each page from disk searching for the first key.
        // This is slower than getFirstKey() due to our locking convention.
        try {
            // Load the high page into memory
            while (true) {
                currentKeyEncoded = externalStore.lastKey();
                currentKey = keyCoder.keyDecode(currentKeyEncoded);

                currentPage = loadPage(currentKey, null);
                if (!currentPage.inTransientState() && currentPage.getNextFirstKey() == null) {
                    break;
                }

            }

            // Find that last key!
            while (true) {
                K prevKey, verifyKey;
                byte[] prevKeyEncoded, verifyKeyEncoded;

                assert (!currentPage.inTransientState());

                if (currentPage.keys() == null) {
                    pullPageFromDisk(currentPage, LockMode.WRITEMODE);
                }

                if (currentPage.size() > 0) {
                    return currentPage.keys().get(currentPage.size() - 1);
                }

                // This loop is needed to detect concurrent page split operations.
                do {
                    prevPage = writeUnlockAndNull(prevPage);

                    prevKeyEncoded = externalStore.lowerKey(currentKeyEncoded);

                    if (prevKeyEncoded == null) {
                        return null;
                    }

                    prevKey = keyCoder.keyDecode(prevKeyEncoded);

                    currentPage = writeUnlockAndNull(currentPage);

                    prevPage = loadPage(prevKey, null);

                    verifyKeyEncoded = externalStore.higherKey(prevKeyEncoded);

                    if (verifyKeyEncoded == null) {
                        assert (prevPage.getNextFirstKey() == null);
                        break;
                    }

                    verifyKey = keyCoder.keyDecode(verifyKeyEncoded);
                }
                while (!currentKey.equals(verifyKey));

                currentPage = prevPage;
                currentKey = prevKey;
                currentKeyEncoded = prevKeyEncoded;
                prevPage = null;
            }
        } finally {
            writeUnlockAndNull(prevPage);
            writeUnlockAndNull(currentPage);
        }
    }

    private class SkipListCacheKeyValue implements Map.Entry<K, V> {

        final K key;
        V value;

        public SkipListCacheKeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }


        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V prevValue = put(key, value);
            this.value = value;
            return prevValue;
        }
    }

    /**
     * Close without scheduling any unfinished background tasks.
     * The background eviction thread(s) are shut down regardless of
     * whether the skiplist exceeds its heap capacity.
     */
    @Override
    public void close() {
        doClose(false, false, CloseOperation.NONE);
    }

    /**
     * Close the cache.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     * @return status code. A status code of 0 indicates success.
     */
    @Override
    public int close(boolean cleanLog, CloseOperation operation) {
        return doClose(cleanLog, false, operation);
    }


    /**
     * Wait for all background tasks to complete.
     * Wait for the background eviction threads to complete
     * purging all necessary pages. This method is intended
     * for JUnit testing. If it is being used in other instances,
     * then perhaps a new method should be introduced instead.
     */
    @VisibleForTesting
    void waitForShutdown() {
        doClose(false, true, CloseOperation.NONE);
    }


    private int doClose(boolean cleanLog, boolean wait, CloseOperation operation) {
        int status = 0;
        if (!shutdownGuard.getAndSet(true)) {
            if (wait) {
                waitForPageEviction();
            }

            shutdownEvictionThreads.set(true);
            waitForEvictionThreads();
            pushAllPagesToDisk();
            if (operation != null && operation.testIntegrity()) {
                int failedPages = testIntegrity(operation.repairIntegrity());
                status = (failedPages > 0) ? 1 : 0;
            }
            closeExternalStore(cleanLog);
            assert(status == 0);
            log.info("pages: encoded=" + numPagesEncoded.get() +
                    " decoded=" + numPagesDecoded.get() +
                    " split=" + numPagesSplit.get());
            if (trackEncodingByteUsage) {
                log.info(MetricsUtil.histogramToString("encodeFirstKeySize", metrics.encodeFirstKeySize));
                log.info(MetricsUtil.histogramToString("encodeNextFirstKeySize", metrics.encodeNextFirstKeySize));
                log.info(MetricsUtil.histogramToString("encodeKeySize", metrics.encodeKeySize));
                log.info(MetricsUtil.histogramToString("encodeValueSize", metrics.encodeValueSize));
                log.info(MetricsUtil.histogramToString("encodePageSize (final)",
                        metrics.encodePageSize));
                log.info(MetricsUtil.histogramToString("numberKeysPerPage",
                        metrics.numberKeysPerPage));
            }
        }
        return status;
    }

    /**
     * Retrieve a BackgroundEvictionTask object from
     * the {@link #evictionTaskQueue} or create a new instance when
     * the queue is empty.
     */
    private BackgroundEvictionTask getEvictionTask() {
        BackgroundEvictionTask task = evictionTaskQueue.poll();
        if (task == null) {
            return new BackgroundEvictionTask(fixedNumberEvictions);
        } else {
            return task;
        }
    }

    /**
     * Place a BackgroundEvictionTask object onto
     * the shared queue so that other threads may
     * re-use this object.
     */
    private void putEvictionTask(BackgroundEvictionTask task) {
        evictionTaskQueue.add(task);
    }

    private void waitForEvictionThreads() {
        purgeThreadPool.shutdown();
        evictionThreadPool.shutdown();

        try {
            purgeThreadPool.awaitTermination(threadPoolWaitShutdownSeconds, TimeUnit.SECONDS);
            evictionThreadPool.awaitTermination(threadPoolWaitShutdownSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }


    public class BackgroundEvictionTask implements Runnable {

        private final int id;

        private final int maxEvictions;

        public BackgroundEvictionTask(int evictions) {
            id = evictionId.getAndIncrement();
            maxEvictions = evictions;
        }

        @Override
        public void run() {
            try {
                if (maxEvictions <= 0) {
                    backgroundEviction();
                } else {
                    fixedNumberEviction(maxEvictions);
                }
            } catch (Exception ex) {
                logException("Uncaught exception in eviction task", ex);
            }
        }

    }

    protected void addToPurgeSet(Page<K, V> page) {
        if (!page.getFirstKey().equals(negInf)) {
            if (purgeSet.add(page.getFirstKey())) {
                purgeSetSize.getAndIncrement();
            }
        }
    }


}

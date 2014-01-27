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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.kv.ExternalPagedStore.ByteStore;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.util.MetricsUtil;
import com.addthis.hydra.store.util.NamedThreadFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class SkipListCache<K, V> implements PagedKeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(SkipListCache.class);

    static final int defaultMaxPages = Parameter.intValue("eps.cache.pages", 50);
    static final int defaultMaxPageEntries = Parameter.intValue("eps.cache.page.entries", 50);
    static final int expirationDelta = Parameter.intValue("cache.expire.delta", 1000);
    private static final int defaultEvictionThreads = Parameter.intValue("cache.threadcount.eviction", 1);
    private static final int fixedNumberEvictions = Parameter.intValue("cache.batch.evictions", 10);
    static final boolean trackEncodingByteUsage = Parameter.boolValue("eps.cache.track.encoding", false);

    /**
     * Used as an absolute delta from maxPages when using that upper bound.
     * Otherwise it's treated as a percentage of maxTotalMemory.
     */
    private static final int shouldEvictDelta = Parameter.intValue("eps.cache.evict.delta", 20);

    final ConcurrentSkipListMap<K, Page<K, V>> cache;

    final ByteStore externalStore;

    private final AtomicBoolean shutdownGuard, shutdownEvictionThreads;

    final BlockingQueue<Page<K, V>> evictionQueue;

    final ConcurrentSkipListSet<K> purgeSet;

    final AtomicInteger purgeSetSize = new AtomicInteger(0);

    /**
     * Used to schedule synchronous page eviction in the
     * {@link #put(Object, Object)} and {@link #remove(Object)}
     * methods when the background eviction threads are behind schedule.
     */
    private final LinkedBlockingQueue<BackgroundEvictionTask> evictionTaskQueue;

    final AtomicInteger cacheSize = new AtomicInteger();
    final AtomicInteger numPagesInMemory = new AtomicInteger();
    final AtomicLong numPagesDeleted = new AtomicLong();
    final AtomicLong numPagesEncoded = new AtomicLong();
    final AtomicLong numPagesDecoded = new AtomicLong();
    final AtomicLong numPagesSplit = new AtomicLong();

    final int mem_page;

    final AtomicLong memoryEstimate = new AtomicLong();

    final AtomicLong estimateCounter = new AtomicLong();

    private static final AtomicInteger evictionId = new AtomicInteger();

    private static final AtomicInteger scopeGenerator = new AtomicInteger();

    final String scope = "SkipListCache" + Integer.toString(scopeGenerator.getAndIncrement());

    final SkipListCacheMetrics metrics = new SkipListCacheMetrics(this);

    private final ScheduledExecutorService evictionThreadPool, purgeThreadPool;

    private final Comparator comparator;

    final KeyCoder<K, V> keyCoder;

    long softTotalMem;
    long maxTotalMem;
    long maxPageMem;
    boolean overrideDefaultMaxPages;
    int estimateInterval;
    int maxPageSize;
    int maxPages;

    private static long globalMaxTotalMem;
    private static long globalSoftTotalMem;

    private static final int evictionThreadSleepMillis = 10;
    private static final int threadPoolWaitShutdownSeconds = 10;

    /**
     * The Builder pattern allows many different variations of a class to
     * be instantiated without the pitfalls of complex constructors. See
     * ''Effective Java, Second Edition.'' Item 2 - "Consider a builder when
     * faced with many constructor parameters."
     */
    public static class Builder<K, V> {

        // Required parameters
        protected final int maxPageSize;
        protected final ByteStore externalStore;
        protected final KeyCoder<K, V> keyCoder;

        // Optional parameters - initialized to default values;
        protected int numEvictionThreads = defaultEvictionThreads;
        protected int maxPages = defaultMaxPages;

        public Builder(KeyCoder<K, V> keyCoder, ByteStore store, int maxPageSize) {
            this.externalStore = store;
            this.maxPageSize = maxPageSize;
            this.keyCoder = keyCoder;
        }

        public Builder(KeyCoder<K, V> keyCoder, ByteStore store, int maxPageSize, int maxPages) {
            this.externalStore = store;
            this.maxPageSize = maxPageSize;
            this.keyCoder = keyCoder;
            this.maxPages = maxPages;
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

        public SkipListCache<K, V> build() {
            return new SkipListCache<>(keyCoder, externalStore, maxPageSize,
                    maxPages, numEvictionThreads);
        }

    }


    public SkipListCache(KeyCoder<K, V> keyCoder, ByteStore externalStore, int maxPageSize,
            int maxPages) {
        this(keyCoder, externalStore, maxPageSize, maxPages,
                defaultEvictionThreads);
    }

    public SkipListCache(KeyCoder<K, V> keyCoder, ByteStore externalStore, int maxPageSize,
            int maxPages, int numEvictionThreads) {
        if (externalStore == null) {
            throw new NullPointerException("externalStore must be non-null");
        }

        if (numEvictionThreads <= 0) {
            throw new IllegalStateException("numEvictionThreads must be a non-negative integer");
        }

        this.keyCoder = keyCoder;
        this.negInf = keyCoder.negInfinity();
        this.cache = new ConcurrentSkipListMap<>();
        this.mem_page = (int) MemoryCounter.estimateSize(Page.measureMemoryEmptyPage());
        this.externalStore = externalStore;
        this.maxPageSize = maxPageSize;
        this.maxPages = maxPages;
        this.shutdownGuard = new AtomicBoolean(false);
        this.shutdownEvictionThreads = new AtomicBoolean(false);
        this.evictionTaskQueue = new LinkedBlockingQueue<>();
        this.purgeSet = new ConcurrentSkipListSet<>();
        this.evictionQueue = new LinkedBlockingQueue<>();

        loadFromExternalStore();

        this.comparator = null;

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
                 " maxPages=" + maxPages + " gztype=" + Page.gztype + " gzlevel=" +
                 Page.gzlevel + " gzbuf=" + Page.gzbuf + " mem[page=" + mem_page + "]");

    }

    @SuppressWarnings("unused")
    public void setMaxPages(int maxPages) {
        this.maxPages = maxPages;
    }

    @SuppressWarnings("unused")
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    final K negInf;

    public final boolean nullRawValue(byte[] value) {
        return (value == null) || keyCoder.nullRawValueInternal(value);
    }

    static enum EvictionStatus {
        // did not attempt eviction
        NO_STATUS,

        // call to page.writeTryLock() failed
        TRYLOCK_FAIL,

        // page is in a transient state.
        TRANSIENT_PAGE,

        // eviction successful
        SUCCESS,

        // page has already been evicted
        EVICTED_PAGE,

        // page is scheduled for deletion
        DELETION_SCHEDULED;

        /**
         * If true then reinsert this page into the eviction queue.
         * NO_STATUS implies we did not attempt eviction.
         * TRYLOCK_FAIL implies we optimistically attempted to call writeTryLock() and failed.
         * DELETION_SCHEDULED implies the page has 0 entries. This page will either
         * move into a transient state or new keys will be inserted into the page.
         * LARGE_PAGE indicates the page is large enough to split.
         */
        boolean needsAdditionalProcessing() {
            return this == NO_STATUS || this == TRYLOCK_FAIL ||
                   this == DELETION_SCHEDULED;
        }

        public boolean completeSuccess() {
            return this == SUCCESS;
        }

        public boolean removePurgeSet() {
            return this == SUCCESS || this == TRANSIENT_PAGE || this == EVICTED_PAGE;
        }
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
                prevEntry = cache.lowerEntry(targetKey);
                prevPage = prevEntry.getValue();
                if (!prevPage.writeTryLock()) {
                    prevPage = null;
                    return EvictionStatus.TRYLOCK_FAIL;
                }
                if (prevPage.inTransientState()) {
                    return EvictionStatus.TRANSIENT_PAGE;
                }

                currentEntry = cache.higherEntry(prevEntry.getKey());
                if (currentEntry != null) {
                    currentPage = currentEntry.getValue();
                    if (!currentPage.writeTryLock()) {
                        currentPage = null;
                        return EvictionStatus.TRYLOCK_FAIL;
                    }
                    int compareKeys = compareKeys(targetKey, currentPage.firstKey);
                    if (compareKeys < 0) {
                        return EvictionStatus.NO_STATUS;
                    } else if (compareKeys == 0 && currentPage.keys == null &&
                               currentPage.state == ExternalMode.DISK_MEMORY_IDENTICAL) {
                        currentPage.state = ExternalMode.MEMORY_EVICTED;
                        cache.remove(targetKey);
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


    class BackgroundEvictionTask implements Runnable {

        private volatile long timeout;

        private final long initialTimeout;

        private final ByteArrayOutputStream byteStream;

        private final int id;

        private final String scope;

        private final int maxEvictions;

        @SuppressWarnings("unused")
        private final Gauge<Long> timeoutGauge;

        BackgroundEvictionTask(int evictions) {
            byteStream = new ByteArrayOutputStream();
            id = evictionId.getAndIncrement();
            maxEvictions = evictions;
            scope = "EvictionTask-" + SkipListCache.this.scope + "-" + id;
            timeoutGauge = Metrics.newGauge(getClass(),
                    "timeout", scope,
                    new Gauge<Long>() {
                        @Override
                        public Long value() {
                            return timeout;
                        }
                    });
            initialTimeout = 10;
            timeout = initialTimeout;
        }

        @Override
        public void run() {
            try {

                if (maxEvictions <= 0) {
                    backgroundEviction();
                } else {
                    fixedNumberEviction();
                }
            } catch (Exception ex) {
                logException("Uncaught exception in skiplist concurrent cache eviction thread", ex);
            }
        }

        private void fixedNumberEviction() {
            for (int i = 0; i < maxEvictions; i++) {
                doEvictPage();
            }
        }

        private void backgroundEviction() {
            while (!shutdownEvictionThreads.get() && shouldEvictPage() && doEvictPage()) ;
        }

        private EvictionStatus attemptPageEviction(Page<K, V> page, IterationMode iteration) {
            if (iteration == IterationMode.OPTIMISTIC) {
                if (!page.writeTryLock()) {
                    return EvictionStatus.TRYLOCK_FAIL;
                }
            } else {
                page.writeLock();
            }

            try {
                if (page.inTransientState()) {
                    return EvictionStatus.TRANSIENT_PAGE;
                }

                assert (!page.splitCondition());

                if (page.size == 0 && !page.firstKey.equals(negInf)) {
                    return EvictionStatus.DELETION_SCHEDULED;
                }

                if (page.keys == null) {
                    addToPurgeSet(page);
                    return EvictionStatus.EVICTED_PAGE;
                }

                pushPageToDisk(page, byteStream);

                addToPurgeSet(page);

                if (iteration == IterationMode.OPTIMISTIC) {
                    timeout = timeout + expirationDelta;
                }

                return EvictionStatus.SUCCESS;
            } finally {
                writeUnlockAndNull(page);
            }
        }

        private void addToPurgeSet(Page<K, V> page) {
            if (!page.firstKey.equals(negInf)) {
                if (purgeSet.add(page.firstKey)) {
                    purgeSetSize.getAndIncrement();
                }
            }
        }

        /**
         * Returns <code>true</code> is a page is evicted and
         * false otherwise.
         */
        private boolean doEvictPage() {
            long referenceTime = generateTimestamp();


            Page<K, V> current = evictionQueue.poll();

            Page<K, V> oldestPage = current;

            // keeps track of the timestamp with the smallest value
            long oldestTimeStamp = (current != null) ? current.timeStamp : 0;

            int counter = 0;

            int numPages = getNumPagesInMemory();

            IterationMode iteration = IterationMode.OPTIMISTIC;

            EvictionStatus status;

            while (iteration != IterationMode.TERMINATION) {
                counter++;

                if (current == null) {
                    return false;
                }

                long timestamp = current.timeStamp;

                status = EvictionStatus.NO_STATUS;

                if (((iteration == IterationMode.OPTIMISTIC) &&
                     ((referenceTime - timestamp) >= timeout)) ||
                    (iteration == IterationMode.PESSIMISTIC)) {
                    status = attemptPageEviction(current, iteration);

                    if (status.completeSuccess()) {
                        return true;
                    }
                }

                if (timestamp < oldestTimeStamp) {
                    oldestTimeStamp = timestamp;
                    oldestPage = current;
                }

                if (status.needsAdditionalProcessing()) {
                    evictionQueue.offer(current);
                }

                if (counter >= numPages) {
                    switch (iteration) {
                        case OPTIMISTIC:
                            iteration = IterationMode.PESSIMISTIC;
                            timeout /= 2;
                            status = attemptPageEviction(oldestPage, iteration);
                            if (status.completeSuccess()) {
                                return true;
                            }
                            referenceTime = generateTimestamp();
                            counter = 0;
                            break;
                        case PESSIMISTIC:
                            iteration = IterationMode.TERMINATION;
                            break;
                    }
                }

                if (iteration != IterationMode.TERMINATION) {
                    current = evictionQueue.poll();
                }
            }

            return false;
        }

    }

    public boolean shouldPurgePage() {
        return purgeSetSize.get() > getNumPagesInMemory();
    }

    public boolean shouldEvictPage() {
        int numPages = getNumPagesInMemory();

        if (maxTotalMem > 0) {
            return getMemoryEstimate() > softTotalMem && numPages > 5;
        } else if (maxPages > 0) {
            return numPages > Math.max(maxPages - shouldEvictDelta, 5);
        } else if (!overrideDefaultMaxPages) {
            return numPages > Math.max(defaultMaxPages - shouldEvictDelta, 5);
        } else {
            return numPages > 0;
        }
    }


    public boolean mustEvictPage() {
        int numPages = getNumPagesInMemory();

        if (overrideDefaultMaxPages) {
            return false;
        } else if (maxTotalMem > 0) {
            return getMemoryEstimate() > maxTotalMem && numPages > 5;
        } else if (maxPages > 0) {
            return numPages > maxPages;
        } else {
            return numPages > defaultMaxPages;
        }
    }

    private static enum IterationMode {
        OPTIMISTIC, PESSIMISTIC, TERMINATION
    }

    /**
     * If the value of the {@link Page#nextFirstKey} field of a Page
     * is detected to be incorrect, then this method will correct that
     * field.
     */
    private void updateNextFirstKey(Page<K, V> prevPage, K newNextFirstKey,
            K targetKey, byte[] encodedTargetKey) {
        assert (prevPage.isWriteLockedByCurrentThread());

        Map.Entry<byte[], byte[]> entry = externalStore.floorEntry(encodedTargetKey);
        K floorKey = keyCoder.keyDecode(entry.getKey());
        if (floorKey.equals(prevPage.firstKey)) {
            if (prevPage.keys == null) {
                pullPageHelper(prevPage, entry.getValue());
            }
            assert (prevPage.nextFirstKey.equals(targetKey));
            prevPage.nextFirstKey = newNextFirstKey;
            if (prevPage.state == ExternalMode.DISK_MEMORY_IDENTICAL) {
                prevPage.state = ExternalMode.DISK_MEMORY_DIRTY;
            }
        } else {
            Page<K, V> diskPage = Page.generateEmptyPage(SkipListCache.this, floorKey);
            diskPage.decode(entry.getValue());
            assert (diskPage.nextFirstKey.equals(targetKey));
            assert (compareKeys(prevPage.firstKey, diskPage.firstKey) <= 0);
            diskPage.nextFirstKey = newNextFirstKey;
            externalStore.put(entry.getKey(), diskPage.encode());
        }
    }

    private void deletePage(final K targetKey) {
        assert (!targetKey.equals(negInf));

        final byte[] encodedTargetKey = keyCoder.keyEncode(targetKey);

        while (true) {

            Page<K, V> prevPage = null, currentPage = null;

            try {

                byte[] prevKeyEncoded = externalStore.lowerKey(encodedTargetKey);

                if (prevKeyEncoded == null) {
                    return;
                }

                K prevKey = keyCoder.keyDecode(prevKeyEncoded);
                prevPage = locatePage(prevKey, LockMode.WRITEMODE);

                if (!prevPage.firstKey.equals(prevKey)) {
                    continue;
                }

                Map.Entry<K, Page<K, V>> currentEntry = cache.higherEntry(prevKey);

                if (currentEntry == null) {
                    return;
                }

                currentPage = currentEntry.getValue();
                currentPage.writeLock();
                if (currentPage.inTransientState()) {
                    continue;
                }
                int compareKeys = compareKeys(targetKey, currentPage.firstKey);
                if (compareKeys > 0) {
                    continue;
                } else if (compareKeys == 0 && currentPage.size == 0) {
                    byte[] verifyPrevKeyEncoded = externalStore.lowerKey(encodedTargetKey);
                    // Test whether the lower key moved while we
                    // were acquiring locks on prevPage and currentPage.
                    if (verifyPrevKeyEncoded == null ||
                        !prevKey.equals(keyCoder.keyDecode(verifyPrevKeyEncoded))) {
                        continue;
                    }
                    externalStore.delete(encodedTargetKey);
                    Page<K, V> prev = cache.remove(targetKey);
                    assert (prev != null);
                    currentPage.state = ExternalMode.DELETED;
                    numPagesInMemory.getAndDecrement();
                    numPagesDeleted.getAndIncrement();
                    prevPage.nextFirstKey = currentPage.nextFirstKey;
                    prevPage.state = ExternalMode.DISK_MEMORY_DIRTY;
                }
                return;
            } finally {
                writeUnlockAndNull(currentPage);
                writeUnlockAndNull(prevPage);
            }
        }
    }

    private void splitPage(Page<K, V> target) {
        Page<K, V> newPage = null;
        try {
            newPage = splitOnePage(target);
            if (target.splitCondition()) {
                splitPage(target);
            }
            if (newPage.splitCondition()) {
                splitPage(newPage);
            }
        } finally {
            writeUnlockAndNull(newPage);
        }
    }

    /**
     * Splits a page in half. The input page must be write locked,
     * hold enough keys to satisfy the split condition, and cannot
     * be in a transient state. The skip-list cache uses the invariant
     * that each page in the cache must have some copy of the page
     * in external storage. We currently use Berkeley DB as the external
     * storage system, which is an append-only database. To conserve
     * disk space we do not store a full page to the database but instead
     * insert (new key, empty page) as a stub into the database.
     *
     * @param target page to split
     */
    private Page<K, V> splitOnePage(Page<K, V> target) {
        assert (target.isWriteLockedByCurrentThread());
        assert (target.splitCondition());
        assert (!target.inTransientState());

        if (target.keys == null) {
            pullPageFromDisk(target, LockMode.WRITEMODE);
        }

        int newSize = target.size / 2;
        int sibSize = target.size - newSize;

        List<K> keyRange = target.keys.subList(newSize, target.size);
        List<V> valueRange = target.values.subList(newSize, target.size);
        List<byte[]> rawValueRange = target.rawValues.subList(newSize, target.size);

        ArrayList<K> sibKeys = new ArrayList<>(keyRange);
        ArrayList<V> sibValues = new ArrayList<>(valueRange);
        ArrayList<byte[]> sibRawValues = new ArrayList<>(rawValueRange);
        K sibMinKey = sibKeys.get(0);

        Page<K, V> sibling = Page.generateSiblingPage(SkipListCache.this,
                sibMinKey, target.nextFirstKey, sibSize, sibKeys, sibValues, sibRawValues);

        sibling.writeLock();

        byte[] encodeKey;
        byte[] placeHolder;

        sibling.state = ExternalMode.DISK_MEMORY_DIRTY;
        target.state = ExternalMode.DISK_MEMORY_DIRTY;

        Page<K, V> prev = cache.putIfAbsent(sibMinKey, sibling);
        if (prev != null) {
            throw new IllegalStateException("Page split " + target.firstKey.toString() +
                                            " resulted in a new page " + sibMinKey.toString() +
                                            " that already exists in cache.");
        }

        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();

        sibling.avgEntrySize = target.avgEntrySize;
        sibling.estimates = target.estimates;
        sibling.estimateTotal = target.estimateTotal;

        target.nextFirstKey = sibMinKey;
        target.size = newSize;

        int prevMem = target.getMemoryEstimate();

        target.updateMemoryEstimate();
        sibling.updateMemoryEstimate();

        int updatedMem = target.getMemoryEstimate() + sibling.getMemoryEstimate();
        updateMemoryEstimate(updatedMem - prevMem);

        encodeKey = keyCoder.keyEncode(sibMinKey);

        placeHolder = Page.generateEmptyPage(SkipListCache.this,
                sibling.firstKey, sibling.nextFirstKey).encode(false);

        externalStore.put(encodeKey, placeHolder);

        evictionQueue.offer(sibling);
        numPagesSplit.getAndIncrement();

        keyRange.clear();
        valueRange.clear();
        rawValueRange.clear();

        return sibling;
    }

    private void logException(String message, Exception ex) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        ex.printStackTrace(printWriter);
        log.warn(message + " : " + result.toString());
    }

    /* ---------------- Comparison utilities -------------- */

    /**
     * Represents a key with a comparator as a Comparable.
     * <p/>
     * Because most sorted collections seem to use natural ordering on
     * Comparables (Strings, Integers, etc), most internal methods are
     * geared to use them. This is generally faster than checking
     * per-comparison whether to use comparator or comparable because
     * it doesn't require a (Comparable) cast for each comparison.
     * (Optimizers can only sometimes remove such redundant checks
     * themselves.) When Comparators are used,
     * ComparableUsingComparators are created so that they act in the
     * same way as natural orderings. This penalizes use of
     * Comparators vs Comparables, which seems like the right
     * tradeoff.
     */
    static final class ComparableUsingComparator<K> implements Comparable<K> {

        final K actualKey;
        final Comparator<? super K> cmp;

        ComparableUsingComparator(K key, Comparator<? super K> cmp) {
            this.actualKey = key;
            this.cmp = cmp;
        }

        public int compareTo(K k2) {
            return cmp.compare(actualKey, k2);
        }
    }

    /**
     * If using comparator, return a ComparableUsingComparator, else
     * cast key as Comparable, which may cause ClassCastException,
     * which is propagated back to caller.
     */
    @SuppressWarnings("unchecked")
    Comparable<? super K> comparable(K key) throws ClassCastException {
        if (key == null) {
            throw new NullPointerException();
        }
        if (comparator != null) {
            return new ComparableUsingComparator<K>(key, comparator);
        } else {
            return (Comparable<? super K>) key;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareKeys(K key1, K key2) {
        if (comparator == null) {
            return ((Comparable<? super K>) key1).compareTo(key2);
        } else {
            return comparator.compare(key1, key2);
        }
    }

    public boolean isReadOnly() {
        return externalStore.isReadOnly();
    }

    // For testing purposes only
    protected void setOverrideDefaultMaxPages() {
        overrideDefaultMaxPages = true;
    }

    /**
     * Invoked by the constructor. If the left sentinel page is not
     * found in the external storage, then create the left sentinel
     * page.
     */
    private void loadFromExternalStore() {
        byte[] encodedFirstKey = externalStore.firstKey();
        Page<K, V> leftSentinel = Page.generateEmptyPage(this, negInf);
        if (encodedFirstKey == null) {
            leftSentinel.initialize();
            byte[] encodeKey = keyCoder.keyEncode(negInf);
            byte[] encodePage = leftSentinel.encode();
            externalStore.put(encodeKey, encodePage);
        } else {
            K firstKey = keyCoder.keyDecode(encodedFirstKey);
            byte[] page = externalStore.get(encodedFirstKey);

            if (firstKey.equals(negInf)) {
                leftSentinel.decode(page);
                updateMemoryEstimate(leftSentinel.getMemoryEstimate());
            } else {
                leftSentinel.initialize();
                leftSentinel.nextFirstKey = firstKey;

                byte[] encodeKey = keyCoder.keyEncode(negInf);
                byte[] encodePage = leftSentinel.encode();
                externalStore.put(encodeKey, encodePage);

                Page<K, V> minPage = Page.generateEmptyPage(this, firstKey);
                minPage.decode(page);

                cache.put(firstKey, minPage);
                updateMemoryEstimate(minPage.getMemoryEstimate());
                cacheSize.getAndIncrement();
                numPagesInMemory.getAndIncrement();
                evictionQueue.offer(minPage);
            }
        }
        cache.put(negInf, leftSentinel);
        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();
        evictionQueue.offer(leftSentinel);
    }

    @SuppressWarnings("unchecked")
    static <K> int binarySearch(ArrayList<K> arrayList, K key, Comparator comparator) {
        if (comparator != null) {
            return Collections.binarySearch(arrayList, key, comparator);
        } else {
            return Collections.binarySearch((ArrayList<Comparable<K>>) arrayList, key);
        }
    }


    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p/>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key} compares
     * equal to {@code k} according to the map's ordering, then this
     * method returns {@code v}; otherwise it returns {@code null}.
     * (There can be at most one such mapping.)
     *
     * @throws ClassCastException   if the specified key cannot be compared
     *                              with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public V get(K key) {
        return doGet(key);
    }

    /**
     * Locate the page that stores the (key, value) pair
     * and retrieve the current value.
     */
    private V doGet(K key) {
        Page<K, V> page = locatePage(key, LockMode.READMODE);
        try {
            if (page.size == 0) {
                return null;
            }
            int offset = binarySearch(page.keys, key, comparator);
            if (offset >= 0) {
                page.fetchValue(offset);

                return page.values.get(offset);
            } else {
                return null;
            }
        } finally {
            page.readUnlock();
        }
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key
     * @throws ClassCastException   if the specified key cannot be compared
     *                              with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(K key, V value) {
        if (value == null) {
            return doRemove(key);
        } else {
            return doPut(key, value);
        }
    }

    public V remove(K key) {
        return doRemove(key);
    }

    /**
     * @param start     lower bound of range deletion
     * @param end       upper bound of range deletion
     * @param inclusive if true then delete the end key
     */
    @Override
    public void removeValues(K start, K end, boolean inclusive) {
        doRemove(start, end, inclusive);
    }

    private V putIntoPage(Page<K, V> page, K key, V value) {
        V prev;
        int offset = binarySearch(page.keys, key, comparator);

        // An existing (key, value) pair is found.
        if (offset >= 0) {
            page.fetchValue(offset);

            prev = page.values.set(offset, value);
            page.rawValues.set(offset, null);

            updateMemoryCounters(page, key, value, prev);
        } else { // An existing (key, value) pair is not found.
            int position = ~offset;

            page.keys.add(position, key);
            page.values.add(position, value);
            page.rawValues.add(position, null);

            prev = null;

            // updateMemoryCounters must be invoked before incrementing size.
            updateMemoryCounters(page, key, value, null);
            page.size++;
        }
        return prev;
    }

    V doPut(K key, V value) {
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
            } else if (page.state == ExternalMode.DISK_MEMORY_IDENTICAL) {
                page.state = ExternalMode.DISK_MEMORY_DIRTY;
            }
        } finally {
            page.writeUnlock();
        }

        return prev;
    }

    void doRemove(K start, K end, boolean inclusive) {
        while (true) {
            if (mustEvictPage()) {
                BackgroundEvictionTask task = getEvictionTask();
                task.run();
                putEvictionTask(task);
            }

            Page<K, V> page = locatePage(start, LockMode.WRITEMODE);
            try {
                int startOffset = binarySearch(page.keys, start, comparator);
                int endOffset = binarySearch(page.keys, end, comparator);
                int pageSize = page.size;

                if (startOffset < 0) {
                    startOffset = ~startOffset;
                }

                if (endOffset < 0) {
                    endOffset = ~endOffset;
                } else if (inclusive) {
                    endOffset++;
                }


                if (startOffset < endOffset) {
                    int memEstimate = page.getMemoryEstimate();
                    int length = (endOffset - startOffset);
                    for (int i = 0; i < length; i++) {
                        page.keys.remove(startOffset);
                        page.values.remove(startOffset);
                        page.rawValues.remove(startOffset);
                    }
                    page.size -= length;

                    if (page.state == ExternalMode.DISK_MEMORY_IDENTICAL) {
                        page.state = ExternalMode.DISK_MEMORY_DIRTY;
                    }

                    page.updateMemoryEstimate();
                    updateMemoryEstimate(page.getMemoryEstimate() - memEstimate);
                }

                if (page.size == 0 && !page.firstKey.equals(negInf)) {
                    K targetKey = page.firstKey;
                    page = writeUnlockAndNull(page);
                    deletePage(targetKey);
                    continue;
                } else if (endOffset == pageSize) {
                    byte[] higherKeyEncoded = externalStore.higherKey(keyCoder.keyEncode(page.firstKey));
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

    V doRemove(K key) {
        if (mustEvictPage()) {
            BackgroundEvictionTask task = getEvictionTask();
            task.run();
            putEvictionTask(task);
        }

        Page<K, V> page = locatePage(key, LockMode.WRITEMODE);
        try {
            if (page.size == 0) {
                if (!page.firstKey.equals(negInf)) {
                    K targetKey = page.firstKey;
                    page = writeUnlockAndNull(page);
                    deletePage(targetKey);
                }

                return null;
            }
            int offset = binarySearch(page.keys, key, comparator);

            // An existing (key, value) pair is found.
            if (offset >= 0) {
                int memEstimate = page.getMemoryEstimate();

                page.fetchValue(offset);

                page.keys.remove(offset);
                page.rawValues.remove(offset);
                V prev = page.values.remove(offset);

                page.size--;

                if (page.state == ExternalMode.DISK_MEMORY_IDENTICAL) {
                    page.state = ExternalMode.DISK_MEMORY_DIRTY;
                }

                page.updateMemoryEstimate();
                updateMemoryEstimate(page.getMemoryEstimate() - memEstimate);

                if (page.size == 0 && !page.firstKey.equals(negInf)) {
                    K targetKey = page.firstKey;
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
     * Internal helper method.
     * If the input page is null then do nothing. Otherwise
     * unlock the page. Always return null.
     */
    private static <K, V> Page<K, V> unlockAndNull(Page<K, V> input, LockMode mode) {
        if (input == null) {
            return null;
        }
        input.modeUnlock(mode);
        return null;
    }

    private static <K, V> Page<K, V> writeUnlockAndNull(Page<K, V> input) {
        return unlockAndNull(input, LockMode.WRITEMODE);
    }

    /**
     * Helper method for loadPage().
     */
    private Page<K, V> loadPageCacheFloorEntry(Page<K, V> current, K externalKey) {
        boolean useHint = false;
        try {
            while (true) {
                Map.Entry<K, Page<K, V>> cacheEntry = cache.floorEntry(externalKey);
                K cacheKey = cacheEntry.getKey();
                Page<K, V> cachePage = cacheEntry.getValue();

                assert (cacheKey.equals(cachePage.firstKey));

                /** If the nearest page in cache equals the new page then return. */
                /** If we did not provide a hint then begin with the nearest page in cache. */
                /** If we provided a hint and it was incorrect then do not use the hint. */
                if (cacheKey.equals(externalKey) || current == null || !cacheKey.equals(current.firstKey)) {
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
     * Helper method for loadPage().
     */
    private Page<K, V> constructNewPage(Page<K, V> current, Page<K, V> next,
            K externalKey, byte[] floorPageEncoded) {
        Page<K, V> newPage = Page.generateEmptyPage(this, externalKey);
        newPage.decode(floorPageEncoded);
        newPage.writeLock();
        assert (newPage.firstKey.equals(externalKey));
        assert (compareKeys(current.firstKey, newPage.firstKey) < 0);
        assert (next == null || compareKeys(next.firstKey, newPage.firstKey) > 0);

        Page<K, V> oldPage = cache.putIfAbsent(externalKey, newPage);
        assert (oldPage == null);

        updateMemoryEstimate(newPage.getMemoryEstimate());
        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();
        evictionQueue.offer(newPage);
        return newPage;
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
    private Page<K, V> loadPage(K key, Page<K, V> current) {
        assert (current == null || current.isWriteLockedByCurrentThread());

        Page<K, V> next = null, cachePage;

        try {
            byte[] encodedKey = keyCoder.keyEncode(key);

            while (true) {
                byte[] externalKeyEncoded = externalStore.floorKey(encodedKey);

                /** Key of the page that will be loaded from disk. */
                K externalKey = keyCoder.keyDecode(externalKeyEncoded);

                assert (current == null || compareKeys(current.firstKey, externalKey) < 0);
                assert (compareKeys(key, externalKey) >= 0);

                // Transfer ownership of the 'current' variable to the inner method
                // to handle failures.
                Page<K, V> currentCopy = current;
                current = null;

                /** Locate the nearest page in cache that less than or equal to the new page. */
                cachePage = loadPageCacheFloorEntry(currentCopy, externalKey);

                if (cachePage.firstKey.equals(externalKey)) {
                    cachePage.timeStamp = generateTimestamp();
                    return cachePage;
                } else {
                    current = cachePage;
                }

                assert (!current.inTransientState());

                findnext:
                while (true) {
                    do {
                        writeUnlockAndNull(next);
                        Map.Entry<K, Page<K, V>> higherEntry = cache.higherEntry(current.firstKey);
                        if (higherEntry == null) {
                            break findnext;
                        }
                        next = higherEntry.getValue();
                        next.writeLock();
                    }
                    while (next.inTransientState());

                    if (compareKeys(next.firstKey, externalKey) >= 0) {
                        break;
                    }
                    current.writeUnlock();
                    current = next;
                    next = null;
                }

                if (next != null && next.firstKey.equals(externalKey)) {
                    current = writeUnlockAndNull(current);
                    cachePage = next;
                    next = null;
                    cachePage.timeStamp = generateTimestamp();
                    return cachePage;
                }

                byte[] floorPageEncoded = externalStore.get(externalKeyEncoded);

                if (floorPageEncoded == null) {
                    current = writeUnlockAndNull(current);
                    next = writeUnlockAndNull(next);
                    continue;
                }

                return constructNewPage(current, next, externalKey, floorPageEncoded);
            }
        } finally {
            writeUnlockAndNull(current);
            writeUnlockAndNull(next);
        }
    }

    /**
     * This method locates a page either in cache or in the external storage.
     * If the page is on disk then it is loaded into memory. The target page
     * is returned and it is either read-locked or write-locked depending on
     * the {@param mode} parameter. It is guaranteed that the page returned
     * is not in a transient state and that it has been loaded into memory,
     * ie. (page.keys != null).
     * <p/>
     * Only returns a page when {@link Page#interval(Comparable)} is
     * true for the <code>key</code> argument.
     * <p/>
     * When searching for a page in order to acquire the write-lock,
     * then it is preferable to call loadPage() if a page hint is available.
     * Otherwise this method should be used.
     */
    Page<K, V> locatePage(K key, LockMode returnMode) {
        LockMode currentMode = returnMode;

        Comparable<? super K> ckey = comparable(key);

        Page<K, V> current = null;

        do {
            unlockAndNull(current, currentMode);

            Map.Entry<K, Page<K, V>> cacheEntry = cache.floorEntry(key);

            current = cacheEntry.getValue();

            current.modeLock(currentMode);

            assert (current.firstKey.equals(cacheEntry.getKey()));

        }
        while (current.inTransientState());

        while (true) {
            assert (!current.inTransientState());

            K currentFirstKey = current.firstKey;

            assert (ckey.compareTo(currentFirstKey) >= 0);

            if (current.keys == null) {
                pullPageFromDisk(current, currentMode);
                // If currentMode is LockMode.READMODE then the lock was dropped and re-acquired.
                // We could be in a transient state.
            }

            if (!current.inTransientState()) {
                if (current.interval(ckey)) {
                    current.timeStamp = generateTimestamp();

                    /**
                     *  Fancy way of asserting that we do not
                     *  hold the READLOCK when we want to return the WRITELOCK.
                     */
                    assert (currentMode != LockMode.READMODE || returnMode != LockMode.WRITEMODE);

                    if (currentMode == LockMode.WRITEMODE && returnMode == LockMode.READMODE) {
                        current.downgradeLock();
                    }
                    return current;
                }
            }

            /**
             * The key was not found in a page on memory.
             * We must load a page from external storage.
             */
            if (!current.inTransientState() && currentMode == LockMode.WRITEMODE) {
                current = loadPage(key, current);
            } else {
                current.modeUnlock(currentMode);
                current = loadPage(key, null);
            }

            currentMode = LockMode.WRITEMODE;

        }
    }

    /**
     * Helper method for {@link #getFirstKey()}.
     */
    private K firstKeyFastPath() {
        Page<K, V> leftSentinel = cache.firstEntry().getValue();
        leftSentinel.readLock();
        try {
            if (leftSentinel.keys == null) {
                pullPageFromDisk(leftSentinel, LockMode.READMODE);
            }

            assert (!leftSentinel.inTransientState());

            if (leftSentinel.size > 0) {
                return leftSentinel.keys.get(0);
            }
        } finally {
            leftSentinel.readUnlock();
        }
        return null;
    }

    @Override
    public K getFirstKey() {
        // Fast path: the first key is located in the left sentinel page
        K fastPath = firstKeyFastPath();
        if (fastPath != null) return fastPath;

        Page<K, V> currentPage = cache.firstEntry().getValue();
        K currentKey = currentPage.firstKey;
        byte[] currentKeyEncoded = keyCoder.keyEncode(currentKey);


        currentPage.writeLock();

        // Slow path: we load each page from disk searching for the first key.
        try {
            while (true) {
                assert (!currentPage.inTransientState());

                if (currentPage.keys == null) {
                    pullPageFromDisk(currentPage, LockMode.WRITEMODE);
                }

                if (currentPage.size > 0) {
                    return currentPage.keys.get(0);
                }

                byte[] nextKeyEncoded = externalStore.higherKey(currentKeyEncoded);

                if (nextKeyEncoded == null) {
                    return null;
                }

                K nextKey = keyCoder.keyDecode(nextKeyEncoded);
                currentPage = loadPage(nextKey, currentPage);
                currentKey = currentPage.firstKey;
                currentKeyEncoded = keyCoder.keyEncode(currentKey);
            }
        } finally {
            currentPage.writeUnlock();
        }
    }

    /**
     * Helper method for {@link #getLastKey()}.
     */
    private K lastKeyFastPath() {
        Page<K, V> maxPage = cache.lastEntry().getValue();
        maxPage.readLock();
        try {
            if (maxPage.keys == null) {
                pullPageFromDisk(maxPage, LockMode.READMODE);
            }

            if (!maxPage.inTransientState() && maxPage.nextFirstKey == null && maxPage.size > 0) {
                return maxPage.keys.get(maxPage.size - 1);
            }
        } finally {
            maxPage.readUnlock();
        }
        return null;
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
                if (!currentPage.inTransientState() && currentPage.nextFirstKey == null) {
                    break;
                }

            }

            // Find that last key!
            while (true) {
                K prevKey, verifyKey;
                byte[] prevKeyEncoded, verifyKeyEncoded;

                assert (!currentPage.inTransientState());

                if (currentPage.keys == null) {
                    pullPageFromDisk(currentPage, LockMode.WRITEMODE);
                }

                if (currentPage.size > 0) {
                    return currentPage.keys.get(currentPage.size - 1);
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
                        assert (prevPage.nextFirstKey == null);
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

    private class SkipListCacheIterator implements Iterator<Map.Entry<K, V>> {

        Page<K, V> page;
        int position;
        long stamp;
        K prevKey;
        K nextKey;
        V nextValue;

        SkipListCacheIterator(K from, boolean inclusive) {
            this.page = locatePage(from, LockMode.READMODE);
            this.prevKey = null;
            this.stamp = -1;

            nextHelper(from, inclusive, false);

        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public Map.Entry<K, V> next() {
            if (nextKey == null) {
                return null;
            }

            prevKey = nextKey;

            Map.Entry<K, V> pair = new SkipListCacheKeyValue(nextKey, nextValue);

            nextHelper(prevKey, false, true);

            return pair;
        }

        @Override
        public void remove() {
            if (prevKey != null) {
                cache.remove(prevKey);
            }
        }

        private void nextHelper(K target, boolean inclusive, boolean acquireLock) {
            if (acquireLock) {
                page.readLock();
            }

            try {
                if (page.keys == null) {
                    pullPageFromDisk(page, LockMode.READMODE);
                    // The readlock was dropped and re-acquired.
                    // We could be in a transient state.
                }

                if (page.inTransientState()) {
                    Page<K, V> newPage = locatePage(target, LockMode.READMODE);

                    assert (!newPage.inTransientState());
                    assert (newPage.keys != null);

                    page = unlockAndNull(page, LockMode.READMODE);
                    page = newPage;
                    stamp = -1;
                }

                if (stamp != page.writeStamp) {
                    position = binarySearch(page.keys, target, comparator);

                    if (position < 0) {
                        position = ~position;
                    } else if (!inclusive) {
                        position = position + 1;
                    }

                    stamp = page.writeStamp;
                } else {
                    position++;
                }

                while (position < page.size && page.values.get(position) == null
                       && nullRawValue(page.rawValues.get(position))) {
                    position++;
                }

                if (position == page.size && !moveForward(target, inclusive)) {
                    return;
                }

                page.fetchValue(position);

                nextKey = page.keys.get(position);
                nextValue = page.values.get(position);

            } finally {
                unlockAndNull(page, LockMode.READMODE);
            }
        }

        /**
         * Finds the next key greater than or equal to the targetKey.
         * If inclusive is false then find the next key greater than
         * the targetKey.
         *
         * @param targetKey begin search with this key
         * @param inclusive search for values can terminate on finding the targetKey
         * @return true if-and-only-if a key is found
         */
        private boolean moveForward(K targetKey, boolean inclusive) {
            while (true) {
                byte[] higherKeyEncoded = externalStore.higherKey(keyCoder.keyEncode(page.firstKey));

                if (higherKeyEncoded == null) {
                    nextKey = null;
                    nextValue = null;
                    return false;
                }

                K higherKey = keyCoder.keyDecode(higherKeyEncoded);

                page = unlockAndNull(page, LockMode.READMODE);

                Page<K, V> higherPage = locatePage(higherKey, LockMode.READMODE);

                assert (!higherPage.inTransientState());
                assert (higherPage.keys != null);

                page = higherPage;

                assert (page.keys != null);

                position = binarySearch(page.keys, targetKey, comparator);

                if (position < 0) {
                    position = ~position;
                } else if (!inclusive) {
                    position = position + 1;
                }

                while (position < page.size && page.values.get(position) == null
                       && nullRawValue(page.rawValues.get(position))) {
                    position++;
                }

                if (position < page.size) {
                    stamp = page.writeStamp;
                    return true;
                }
            }
        }

    }

    @Override
    public Iterator<Map.Entry<K, V>> range(K start, boolean inclusive) {
        return new SkipListCacheIterator(start, inclusive);
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public V getValue(K key) {
        return get(key);
    }

    @Override
    public V getPutValue(K key, V val) {
        return put(key, val);
    }

    @Override
    public V getRemoveValue(K key) {
        return remove(key);
    }

    @Override
    public void putValue(K key, V val) {
        put(key, val);
    }

    @Override
    public void removeValue(K key) {
        remove(key);
    }


    public long getMemoryEstimate() {
        return memoryEstimate.get() + getNumPagesInMemory() * mem_page;
    }

    void updateMemoryEstimate(int delta) {
        long est = memoryEstimate.addAndGet(delta);
        assert (est >= 0);
    }

    private void updateMemoryCounters(Page<K, V> page, K key, V value, V prev) {
        /** for memory estimation, the replacement gets 2x weighting */

        if (prev == null) {
            page.updateAverage(key, value, 1);
        } else {
            page.updateAverage(key, value, 2);
        }

    }

    private void pushPageToDisk(Page<K, V> current, ByteArrayOutputStream byteStream) {

        assert (current.isWriteLockedByCurrentThread());

        assert (!current.inTransientState());
        assert (current.keys != null);

        if (current.state == ExternalMode.DISK_MEMORY_DIRTY) {

            // flush to external storage
            byte[] encodeKey = keyCoder.keyEncode(current.firstKey);
            byte[] encodePage = current.encode(byteStream);

            externalStore.put(encodeKey, encodePage);

            current.state = ExternalMode.DISK_MEMORY_IDENTICAL;
        }

        updateMemoryEstimate(-current.getMemoryEstimate());
        current.keys.clear();
        current.values.clear();
        current.rawValues.clear();
        current.keys = null;
        current.values = null;
        current.rawValues = null;
        numPagesInMemory.getAndDecrement();
    }

    private void pullPageHelper(Page<K, V> current, byte[] page) {
        assert (current.isWriteLockedByCurrentThread());

        current.decode(page);

        updateMemoryEstimate(current.getMemoryEstimate());
        evictionQueue.offer(current);
        numPagesInMemory.getAndIncrement();
    }

    private void pullPageFromDisk(Page<K, V> current, LockMode mode) {

        if (mode == LockMode.READMODE) {
            current.readUnlock();
            current.writeLock();
        }

        try {
            assert (current.isWriteLockedByCurrentThread());

            if (current.inTransientState()) {
                return;
            }

            if (current.keys == null) {

                byte[] encodeKey = keyCoder.keyEncode(current.firstKey);
                byte[] page = externalStore.get(encodeKey);

                pullPageHelper(current, page);
            }
        } finally {
            if (mode == LockMode.READMODE) {
                current.downgradeLock();
            }
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

    private boolean pushAllPagesToDiskAssertion() {
        byte[] firstKeyEncoded = externalStore.firstKey();
        K firstKey = keyCoder.keyDecode(firstKeyEncoded);
        return firstKey.equals(negInf);
    }

    private void pushAllPagesToDisk() {
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        for (Page<K, V> page : evictionQueue) {

            page.writeLock();

            if (!page.inTransientState() && page.keys != null) {
                pushPageToDisk(page, byteStream);
            }

            page.writeUnlock();
        }

        assert (pushAllPagesToDiskAssertion());
    }

    /**
     * This method is intended for internal use and unit testing purposes only.
     */
    protected void waitForPageEviction() {

        while (shouldEvictPage()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
    }


    /**
     * Close the external store.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     **/
    private void closeExternalStore(boolean cleanLog) {
        externalStore.close(cleanLog);
    }

    @SuppressWarnings("unused")
    int getCacheSize() {
        return cacheSize.get();
    }

    public int getNumPagesInMemory() {
        return numPagesInMemory.get();
    }

    /**
     * Counts the key/data pairs in the database. This operation is faster than
     * obtaining a count from a cursor based scan of the database, and will not
     * perturb the current contents of the cache. However, the count is not
     * guaranteed to be accurate if there are concurrent updates. Note that
     * this method does scan a significant portion of the database and should
     * be considered a fairly expensive operation.
     * <p/>
     * <p>A count of the key/data pairs in the database is returned without
     * adding to the cache.  The count may not be accurate in the face of
     * concurrent update operations in the database.</p>
     */
    @SuppressWarnings("unused")
    public long getNumPagesOnDisk() {
        return externalStore.count();
    }

    @SuppressWarnings("unused")
    public long getNumPagesDeleted() {
        return numPagesDeleted.get();
    }

    /**
     * Returns timestamps that are applied whenever a page is accessed.
     * <p/>
     * System.nanoTime() resolution has been found to improve the
     * performance of the WS-CLOCK eviction algorithm. If the performance
     * overhead of System.nanoTime() is unacceptable then perhaps
     * a microsecond precision version of JitterClock needs to be implemented.
     */
    static long generateTimestamp() {
        return System.nanoTime();
    }

    @Override
    public void setMaxPageMem(long maxPageMem) {
        this.maxPageMem = maxPageMem;
    }

    @Override
    public void setMaxTotalMem(long maxTotalMem) {
        this.maxTotalMem = maxTotalMem;
        this.softTotalMem = maxTotalMem - (long) ((1.0d / shouldEvictDelta) * maxTotalMem);
        globalMaxTotalMem = Math.max(globalMaxTotalMem, maxTotalMem);
        globalSoftTotalMem = Math.max(globalSoftTotalMem, softTotalMem);
        if (isReadOnly()) {
            this.maxTotalMem = globalMaxTotalMem;
            this.softTotalMem = globalSoftTotalMem;
        }
    }

    @Override
    public void setMemEstimateInterval(int interval) {
        this.estimateInterval = interval;
    }

    /**
     * Emit a log message that a page has been detected with a null nextFirstKey
     * and the page is not the largest page in the database.
     *
     * @param repair   if true then repair the page
     * @param counter  page number.
     * @param page     contents of the page.
     * @param key      key associated with the page.
     * @param nextKey  key associated with the next page.
     */
    private void missingNextFirstKey(final boolean repair, final int counter, Page<K,V> page,
            final K key, final K nextKey) {
        log.warn("On page {} the firstKey is {} " +
                 " the length is {} " +
                 " the nextFirstKey is null and the next page" +
                 " is associated with key {}.",
                counter, page.firstKey, page.size, nextKey);
        if (repair) {
            log.info("Repairing nextFirstKey on page {}.", counter);
            page.nextFirstKey = nextKey;
            byte[] pageEncoded = page.encode();
            externalStore.put(keyCoder.keyEncode(key), pageEncoded);
        }
    }

    /**
     * Emit a log message that a page has been detected with an incorrect nextFirstKey
     * and the page is not the largest page in the database.
     *
     * @param repair   if true then repair the page and possibly move entries to the next page
     * @param counter  page number.
     * @param page     contents of the page.
     * @param key      key associated with the page.
     * @param nextKey  key associated with the next page.
     */
    private void invalidNextFirstKey(final boolean repair, final int counter, Page<K,V> page,
            final K key, final K nextKey) {
        int compareTo = compareKeys(page.nextFirstKey, nextKey);
        char direction = compareTo > 0 ? '>' : '<';
        log.warn("On page " + counter + " the firstKey is " +
                 page.firstKey + " the length is " + page.size +
                 " the nextFirstKey is " + page.nextFirstKey +
                 " which is " + direction + " the next page is associated with key " + nextKey);
        if (repair) {
            log.info("Repairing nextFirstKey on page {}.", counter);
            boolean pageTransfer = false;
            page.nextFirstKey = nextKey;
            Page<K,V> nextPage = Page.generateEmptyPage(this, nextKey);
            byte[] encodedNextPage = externalStore.get(keyCoder.keyEncode(nextKey));
            nextPage.decode(encodedNextPage);

            for(int i = 0; i < page.size; i++) {
                K testKey = page.keys.get(i);
                if (compareKeys(testKey, nextKey) >= 0) {
                    log.info("Moving key {} on page {}.", i, counter);
                    page.fetchValue(i);
                    V value = page.values.get(i);
                    putIntoPage(nextPage, testKey, value);
                    page.keys.remove(i);
                    page.rawValues.remove(i);
                    page.values.remove(i);
                    page.size--;
                    i--;
                    pageTransfer = true;
                }
            }

            byte[] pageEncoded = page.encode();
            externalStore.put(keyCoder.keyEncode(key), pageEncoded);
            if (pageTransfer) {
                encodedNextPage = nextPage.encode();
                externalStore.put(keyCoder.keyEncode(nextKey), encodedNextPage);
            }
        }

    }

    public int testIntegrity(boolean repair) {
        int counter = 0;
        int failedPages = 0;
        byte[] encodedKey = externalStore.firstKey();
        byte[] encodedPage = externalStore.get(encodedKey);
        K key = keyCoder.keyDecode(encodedKey);
        while(encodedKey != null) {
            counter++;
            Page<K, V> page = Page.generateEmptyPage(this, key);
            byte[] encodedNextKey = externalStore.higherKey(encodedKey);
            if (encodedNextKey != null) {
                page.decode(encodedPage);
                K nextKey = keyCoder.keyDecode(encodedNextKey);
                if (page.nextFirstKey == null) {
                    missingNextFirstKey(repair, counter, page, key, nextKey);
                    failedPages++;
                } else if (!page.nextFirstKey.equals(nextKey)) {
                    invalidNextFirstKey(repair, counter, page, key, nextKey);
                    failedPages++;
                }
                key = nextKey;
                encodedPage = externalStore.get(encodedNextKey);
            }
            encodedKey = encodedNextKey;
            if (counter % 10000 == 0) {
                log.info("Scanned " + counter + " pages. Detected " + failedPages + " failed pages.");
            }
        }
        log.info("Scan complete. Scanned " + counter + " pages. Detected " + failedPages + " failed pages.");
        return failedPages;
    }

}

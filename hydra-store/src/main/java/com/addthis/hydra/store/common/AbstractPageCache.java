package com.addthis.hydra.store.common;

import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.skiplist.LockMode;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The abstract implementation of {@link PagedKeyValueStore} extended by
 * {@link com.addthis.hydra.store.nonconcurrent.NonConcurrentPageCache} and
 * {@link com.addthis.hydra.store.skiplist.SkipListCache}
 *
 * This class is optionally thread safe.  if the implantation intends
 * to concurrently access pages stored in this class then this class
 * must be initialized with {@code useLocks} set to true
 *
 * @param <K> the key used to get/put values onto pages maintained by the cache
 * @param <V> the value which must extend {@link BytesCodable}
 */
public abstract class AbstractPageCache<K, V extends BytesCodable> implements PagedKeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractPageCache.class);

    public static final int defaultMaxPages = Parameter.intValue("eps.cache.pages", 50);
    public static final int defaultMaxPageEntries = Parameter.intValue("eps.cache.page.entries", 50);
    public static final int expirationDelta = Parameter.intValue("cache.expire.delta", 1000);
    public static final boolean trackEncodingByteUsage = Parameter.boolValue("eps.cache.track.encoding", false);

    public final AtomicLong memoryEstimate = new AtomicLong();
    public final KeyCoder<K, V> keyCoder;
    public final String scope;
    public final AtomicInteger cacheSize = new AtomicInteger();
    public final AtomicInteger numPagesInMemory = new AtomicInteger();
    public final AtomicLong numPagesDeleted = new AtomicLong();
    public final AtomicLong numPagesEncoded = new AtomicLong();
    public final AtomicLong numPagesDecoded = new AtomicLong();
    public final AtomicLong numPagesSplit = new AtomicLong();

    private final ConcurrentSkipListMap<K, Page<K, V>> cache;
    private final BlockingQueue<Page<K, V>> evictionQueue;

    protected final ByteStore externalStore;

    public final AtomicBoolean shutdownGuard;
    public final AtomicBoolean shutdownEvictionThreads;

    public final int mem_page;

    public final K negInf;

    public final PageFactory<K, V> pageFactory;

    public final PageCacheMetrics<K, V> metrics = new PageCacheMetrics<>(this);

    public final AtomicLong estimateCounter = new AtomicLong();

    public static final AtomicInteger evictionId = new AtomicInteger();

    public static final AtomicInteger scopeGenerator = new AtomicInteger();

    /**
     * Used as an absolute delta from maxPages when using that upper bound.
     * Otherwise it's treated as a percentage of maxTotalMemory.
     */
    public static final int shouldEvictDelta = Parameter.intValue("eps.cache.evict.delta", 20);
    public static final int fixedNumberEvictions = Parameter.intValue("cache.batch.evictions", 100);

    public final Comparator comparator;

    private static long globalMaxTotalMem;
    private static long globalSoftTotalMem;


    private final boolean useLocks;

    private long softTotalMem;
    private long maxTotalMem;
    private long maxPageMem;
    private boolean overrideDefaultMaxPages;
    private int estimateInterval;
    private int maxPageSize;
    private int maxPages;

    @SuppressWarnings("unchecked")
    protected static <K> int binarySearch(ArrayList<K> arrayList, K key, Comparator comparator) {
        if (comparator != null) {
            return Collections.binarySearch(arrayList, key, comparator);
        } else {
            return Collections.binarySearch((ArrayList<Comparable<K>>) arrayList, key);
        }
    }


    public int getEstimateInterval() {
        return estimateInterval;
    }

    public long getMaxPageMem() {
        return maxPageMem;
    }

    public long getMaxPageSize() {
        return maxPageSize;
    }

    public ConcurrentSkipListMap<K, Page<K, V>> getCache() {
        return cache;
    }

    public BlockingQueue<Page<K, V>> getEvictionQueue() {
        return evictionQueue;
    }

    protected enum EvictionStatus {
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
         */
        public boolean needsAdditionalProcessing() {
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

    protected enum IterationMode {
        OPTIMISTIC, PESSIMISTIC, TERMINATION
    }

    public AbstractPageCache(KeyCoder<K, V> keyCoder, ByteStore externalStore,
                             PageFactory<K, V> pageFactory, int maxPageSize, int maxPages,
                             boolean useLocks) {
        if (externalStore == null) {
            throw new NullPointerException("externalStore must be non-null");
        }
        this.keyCoder = keyCoder;
        scope = this.getClass().getSimpleName() + Integer.toString(scopeGenerator.getAndIncrement());
        this.mem_page = (int) MemoryCounter.estimateSize(pageFactory.measureMemoryEmptyPage(PageEncodeType.defaultType()));
        this.pageFactory = pageFactory;

        this.negInf = keyCoder.negInfinity();
        this.cache = new ConcurrentSkipListMap<>();
        this.externalStore = externalStore;
        this.maxPageSize = maxPageSize;
        this.maxPages = maxPages;
        this.shutdownGuard = new AtomicBoolean(false);
        this.evictionQueue = new LinkedBlockingQueue<>();
        this.comparator = null;
        // if we are using locks then we will have eviction threads that need to be shutdown
        this.shutdownEvictionThreads = new AtomicBoolean(useLocks);
        this.useLocks = useLocks;
        loadFromExternalStore();

    }

    public long getMemoryEstimate() {
        return memoryEstimate.get() + getNumPagesInMemory() * mem_page;
    }

    public int getNumPagesInMemory() {
        return numPagesInMemory.get();
    }

    @Override
    @SuppressWarnings("unused")
    public void setMaxPages(int maxPages) {
        this.maxPages = maxPages;
    }

    @Override
    @SuppressWarnings("unused")
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public final boolean nullRawValue(byte[] value) {
        return (value == null);
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
     * Close the external store.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     **/
    protected void closeExternalStore(boolean cleanLog) {
        externalStore.close(cleanLog);
    }

    @SuppressWarnings("unused")
    int getCacheSize() {
        return cacheSize.get();
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
     * <p>
     * System.nanoTime() resolution has been found to improve the
     * performance of the WS-CLOCK eviction algorithm. If the performance
     * overhead of System.nanoTime() is unacceptable then perhaps
     * a microsecond precision version of JitterClock needs to be implemented.
     */
    public static long generateTimestamp() {
        return System.nanoTime();
    }

    public boolean isReadOnly() {
        return externalStore.isReadOnly();
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


    protected void pushPageToDisk(Page<K, V> current, ByteBufOutputStream byteStream) {

        assert (!useLocks || current.isWriteLockedByCurrentThread());
        assert (!current.inTransientState());
        assert (current.keys() != null);

        if (current.getState() == ExternalMode.DISK_MEMORY_DIRTY) {

            // flush to external storage
            byte[] encodeKey = keyCoder.keyEncode(current.getFirstKey());
            byte[] encodePage = current.encode(byteStream);

            externalStore.put(encodeKey, encodePage);

            current.setState(ExternalMode.DISK_MEMORY_IDENTICAL);
        }

        updateMemoryEstimate(-current.getMemoryEstimate());
        current.keys().clear();
        current.values().clear();
        current.rawValues().clear();
        current.setKeys(null);
        current.setValues(null);
        current.setRawValues(null);
        numPagesInMemory.getAndDecrement();
    }

    protected void updateMemoryEstimate(int delta) {
        long est = memoryEstimate.addAndGet(delta);
        assert (est >= 0);
    }

    protected void pullPageHelper(Page<K, V> current, byte[] page) {
        assert (!useLocks || current.isWriteLockedByCurrentThread());
        current.decode(page);
        getEvictionQueue().offer(current);
        updateMemoryEstimate(current.getMemoryEstimate());
        numPagesInMemory.getAndIncrement();
    }

    protected void pullPageFromDisk(Page<K, V> current, LockMode mode) {

        if (useLocks && mode == LockMode.READMODE) {
            current.readUnlock();
            current.writeLock();
        }

        try {
            assert (!useLocks || current.isWriteLockedByCurrentThread());
            if (current.inTransientState()) {
                return;
            }

            if (current.keys() == null) {

                byte[] encodeKey = keyCoder.keyEncode(current.getFirstKey());
                byte[] page = externalStore.get(encodeKey);

                pullPageHelper(current, page);
            }
        } finally {
            if (useLocks && mode == LockMode.READMODE) {
                current.downgradeLock();
            }
        }
    }


    protected void updateMemoryCounters(Page<K, V> page, K key, V value, V prev) {
        /** for memory estimation, the replacement gets 2x weighting */
        if (prev == null) {
            page.updateAverage(key, value, 1);
        } else {
            page.updateAverage(key, value, 2);
        }

    }

    protected void logException(String message, Exception ex) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        ex.printStackTrace(printWriter);
        log.warn(message + " : " + result.toString());
    }


    public void fixedNumberEviction(int numEvictions) {
        ByteBufOutputStream byteStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
        try {
            for (int i = 0; i < numEvictions; i++) {
                doEvictPage(byteStream);
            }
        } finally {
            byteStream.buffer().release();
        }
    }

    public EvictionStatus attemptPageEviction(Page<K, V> page, IterationMode iteration, ByteBufOutputStream byteStream) {
        if (useLocks) {
            if (page.isReadLockedByCurrentThread() || page.isWriteLockedByCurrentThread()) {
                return EvictionStatus.NO_STATUS;
            }
            if (iteration == IterationMode.OPTIMISTIC) {
                if (!page.writeTryLock()) {
                    return EvictionStatus.TRYLOCK_FAIL;
                }
            } else {
                page.writeLock();
            }
        }

        try {
            if (page.inTransientState()) {
                return EvictionStatus.TRANSIENT_PAGE;
            }

            assert (!page.splitCondition());

            if (page.size() == 0 && !page.getFirstKey().equals(negInf)) {
                return EvictionStatus.DELETION_SCHEDULED;
            }

            if (page.keys() == null) {
                addToPurgeSet(page);
                return EvictionStatus.EVICTED_PAGE;
            }

            pushPageToDisk(page, byteStream);

            addToPurgeSet(page);

            return EvictionStatus.SUCCESS;
        } finally {
            if (useLocks) {
                writeUnlockAndNull(page);
            }
        }
    }

    protected abstract void addToPurgeSet(Page<K, V> page);



    /**
     * Returns <code>true</code> is a page is evicted and
     * false otherwise.
     *
     * @param byteStream
     */
    protected boolean doEvictPage(ByteBufOutputStream byteStream) {
        long referenceTime = generateTimestamp();
        int timeout = 10;

        Page<K, V> current = getEvictionQueue().poll();

        Page<K, V> oldestPage = current;

        // keeps track of the timestamp with the smallest value
        long oldestTimeStamp = (current != null) ? current.getTimeStamp() : 0;

        int counter = 0;

        int numPages = getNumPagesInMemory();

        IterationMode iteration = IterationMode.OPTIMISTIC;

        EvictionStatus status;

        while (iteration != IterationMode.TERMINATION) {
            counter++;

            if (current == null) {
                return false;
            }

            long timestamp = current.getTimeStamp();

            status = EvictionStatus.NO_STATUS;

            if (((iteration == IterationMode.OPTIMISTIC) &&
                    ((referenceTime - timestamp) >= timeout)) ||
                    (iteration == IterationMode.PESSIMISTIC)) {
                status = attemptPageEviction(current, iteration, byteStream);

                if (status.completeSuccess()) {
                    return true;
                }
            }

            if (timestamp < oldestTimeStamp) {
                oldestTimeStamp = timestamp;
                oldestPage = current;
            }

            if (status.needsAdditionalProcessing()) {
                getEvictionQueue().offer(current);
            }

            if (counter >= numPages) {
                switch (iteration) {
                    case OPTIMISTIC:
                        iteration = IterationMode.PESSIMISTIC;
                        timeout /= 2;
                        status = attemptPageEviction(oldestPage, iteration, byteStream);
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
                current = getEvictionQueue().poll();
            }
        }

        return false;
    }


    protected Page<K, V> splitPage(Page<K, V> target) {
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
            if (useLocks) {
                writeUnlockAndNull(newPage);
            }
        }
        return newPage;
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
        assert (target.splitCondition());
        assert (!target.inTransientState());

        if (target.keys() == null) {
            pullPageFromDisk(target, useLocks ? LockMode.WRITEMODE : null);
        }

        int newSize = target.size() / 2;
        int sibSize = target.size() - newSize;

        List<K> keyRange = target.keys().subList(newSize, target.size());
        List<V> valueRange = target.values().subList(newSize, target.size());
        List<byte[]> rawValueRange = target.rawValues().subList(newSize, target.size());

        ArrayList<K> sibKeys = new ArrayList<>(keyRange);
        ArrayList<V> sibValues = new ArrayList<>(valueRange);
        ArrayList<byte[]> sibRawValues = new ArrayList<>(rawValueRange);
        K sibMinKey = sibKeys.get(0);

        Page<K, V> sibling = pageFactory.generateSiblingPage(this,
                sibMinKey, target.getNextFirstKey(), sibSize, sibKeys, sibValues, sibRawValues, target.getEncodeType());
        if (useLocks) {
            sibling.writeLock();
        }

        byte[] encodeKey;
        byte[] placeHolder;

        sibling.setState(ExternalMode.DISK_MEMORY_DIRTY);
        target.setState(ExternalMode.DISK_MEMORY_DIRTY);

        Page<K, V> prev = getCache().putIfAbsent(sibMinKey, sibling);
        if (prev != null) {
            throw new IllegalStateException("Page split " + target.getFirstKey().toString() +
                    " resulted in a new page " + sibMinKey.toString() +
                    " that already exists in cache.");
        }

        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();

        sibling.setAvgEntrySize(target.getAvgEntrySize());
        sibling.setEstimates(target.getEstimates());
        sibling.setEstimateTotal(target.getEstimateTotal());

        target.setNextFirstKey(sibMinKey);
        target.setSize(newSize);

        int prevMem = target.getMemoryEstimate();

        target.updateMemoryEstimate();
        sibling.updateMemoryEstimate();

        int updatedMem = target.getMemoryEstimate() + sibling.getMemoryEstimate();
        updateMemoryEstimate(updatedMem - prevMem);

        encodeKey = keyCoder.keyEncode(sibMinKey);

        ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(ByteBufAllocator.DEFAULT.buffer());
        try {
            placeHolder = pageFactory.generateEmptyPage(this,
                    sibling.getFirstKey(), sibling.getNextFirstKey(), sibling.getEncodeType()).encode(byteBufOutputStream, false);
        } finally {
            byteBufOutputStream.buffer().release();
        }
        externalStore.put(encodeKey, placeHolder);
        getEvictionQueue().offer(sibling);
        numPagesSplit.getAndIncrement();

        keyRange.clear();
        valueRange.clear();
        rawValueRange.clear();

        return sibling;
    }


    /* ---------------- Comparison utilities -------------- */

    /**
     * Represents a key with a comparator as a Comparable.
     * <p>
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

        @Override
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
    protected Comparable<K> comparable(K key) throws ClassCastException {
        if (key == null) {
            throw new NullPointerException();
        }
        if (comparator != null) {
            return new ComparableUsingComparator<K>(key, comparator);
        } else {
            return (Comparable<K>) key;
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

    /* ---------------- End Comparison utilities -------------- */


    // For testing purposes only
    @VisibleForTesting
    public void setOverrideDefaultMaxPages() {
        overrideDefaultMaxPages = true;
    }

    /**
     * Invoked by the constructor. If the left sentinel page is not
     * found in the external storage, then create the left sentinel
     * page.
     */
    public void loadFromExternalStore() {
        byte[] encodedFirstKey = externalStore.firstKey();
        Page<K, V> leftSentinel = pageFactory.generateEmptyPage(this, negInf, PageEncodeType.defaultType());
        ByteBufOutputStream byteBufOutputStream = null;
        try {
            if (encodedFirstKey == null) { // effectively externalStore.isEmpty() but more efficient than using count()
                byteBufOutputStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
                leftSentinel.initialize();
                byte[] encodeKey = keyCoder.keyEncode(negInf);
                byte[] encodePage = leftSentinel.encode(byteBufOutputStream);
                externalStore.put(encodeKey, encodePage);
            } else {
                K firstKey = keyCoder.keyDecode(encodedFirstKey);
                byte[] page = externalStore.get(encodedFirstKey);

                if (firstKey.equals(negInf)) {
                    leftSentinel.decode(page);
                    updateMemoryEstimate(leftSentinel.getMemoryEstimate());
                } else {
                    byteBufOutputStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
                    leftSentinel.initialize();
                    leftSentinel.setNextFirstKey(firstKey);

                    byte[] encodeKey = keyCoder.keyEncode(negInf);
                    byte[] encodePage = leftSentinel.encode(byteBufOutputStream);
                    externalStore.put(encodeKey, encodePage);

                    Page<K, V> minPage = pageFactory.generateEmptyPage(this, firstKey, leftSentinel.getEncodeType());
                    minPage.decode(page);

                    getCache().put(firstKey, minPage);
                    updateMemoryEstimate(minPage.getMemoryEstimate());
                    cacheSize.getAndIncrement();
                    numPagesInMemory.getAndIncrement();
                    getEvictionQueue().offer(minPage);
                }
            }
        } finally {
            if (byteBufOutputStream != null) {
                byteBufOutputStream.buffer().release();
            }
        }
        getCache().put(negInf, leftSentinel);
        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();
        getEvictionQueue().offer(leftSentinel);
    }


    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p>
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
    protected V doGet(K key) {
        Page<K, V> page = locatePage(key, LockMode.READMODE);
        try {
            if (page.size() == 0) {
                return null;
            }
            int offset = binarySearch(page.keys(), key, comparator);
            if (offset >= 0) {
                page.fetchValue(offset);
                return page.values().get(offset);
            } else {
                return null;
            }
        } finally {
            if (useLocks) {
                page.readUnlock();
            }
        }
    }

    /**
     * This method locates a page either in cache or in the external storage.
     * If the page is on disk then it is loaded into memory. The target page
     * is returned and if locks are in use it is either read-locked or
     * write-locked depending on the {@param mode} parameter.
     * It is guaranteed that the page returned
     * is not in a transient state and that it has been loaded into memory,
     * ie. (page.keys != null).
     * <p>
     * Only returns a page when {@link Page#interval(Comparable)} is
     * true for the <code>key</code> argument.
     * <p>
     * When searching for a page in order to acquire the write-lock,
     * then it is preferable to call loadPage() if a page hint is available.
     * Otherwise this method should be used.
     */
    protected Page<K, V> locatePage(K key, LockMode returnMode) {
        return locatePage(key, returnMode, false);
    }

    protected Page<K, V> locatePage(K key) {
        return locatePage(key, LockMode.NONE);
    }

    protected Page<K, V> locatePage(K key, LockMode returnMode, boolean exact) {
        LockMode currentMode = returnMode;

        Comparable<K> ckey = comparable(key);

        Page<K, V> current = null;

        do {
            if (useLocks) {
                unlockAndNull(current, currentMode);
            }
            Map.Entry<K, Page<K, V>> cacheEntry = getCache().floorEntry(key);

            current = cacheEntry.getValue();

            if (useLocks) {
                current.modeLock(currentMode);
            }

            assert (current.getFirstKey().equals(cacheEntry.getKey()));

        }
        while (current.inTransientState());

        boolean pageLoad = false;

        while (true) {
            K currentFirstKey = current.getFirstKey();

            assert (ckey.compareTo(currentFirstKey) >= 0);

            if (current.keys() == null) {
                pullPageFromDisk(current, currentMode);
                // If currentMode is LockMode.READMODE then the lock was dropped and re-acquired.
                // We could be in a transient state.
            }

            if (!current.inTransientState()) {
                boolean returnPage = false;
                if (!exact && current.interval(ckey)) {
                    returnPage = true;
                }
                if (exact) {
                    if (current.getFirstKey().equals(key)) {
                        returnPage = true;
                    } else if (pageLoad) {
                        if (useLocks) {
                            current.modeUnlock(currentMode);
                        }
                        return null;
                    }
                }
                if (returnPage) {
                    current.setTimeStamp(generateTimestamp());

                    if (useLocks && (currentMode == LockMode.WRITEMODE && returnMode == LockMode.READMODE)) {
                        current.downgradeLock();
                    }

                    return current;
                }
            }

            /**
             * The key was not found in a page on memory.
             * We must load a page from external storage.
             */
            if (!current.inTransientState() && (!useLocks || currentMode == LockMode.WRITEMODE)) {
                current = loadPage(key, current);
            } else {
                if (useLocks) {
                    current.modeUnlock(currentMode);
                }
                current = loadPage(key, null);
                pageLoad = true;
            }
            currentMode = LockMode.WRITEMODE;
        }
    }

    /**
     * This method loads a page from the external storage if that page is not
     * found in the memory cache. The page that is loaded has the floor key
     * (greatest key less than or equal to) of the input key. Current can
     * be used as a hint to locate the new page. If current is non-null then
     * it must be write-locked.
     * <p>
     * The target page is returned and it is either read-locked or write-locked
     * depending on the {@param mode} parameter. It is guaranteed that the
     * return page is not in a transient state. It is not guaranteed that the
     * return page has been loaded into memory, ie. (page.keys != null).
     */
    protected Page<K, V> loadPage(K key, Page<K, V> current) {
        Page<K, V> next = null, cachePage;

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
                    Map.Entry<K, Page<K, V>> higherEntry = getCache().higherEntry(current.getFirstKey());
                    if (higherEntry == null) {
                        break findnext;
                    }
                    next = higherEntry.getValue();
                }
                while (next.inTransientState());

                if (compareKeys(next.getFirstKey(), externalKey) >= 0) {
                    break;
                }
                current = next;
                next = null;
            }

            if (next != null && next.getFirstKey().equals(externalKey)) {
                cachePage = next;
                cachePage.setTimeStamp(generateTimestamp());
                return cachePage;
            }

            byte[] floorPageEncoded = externalStore.get(externalKeyEncoded);

            if (floorPageEncoded == null) {
                continue;
            }

            Page<K, V> newPage = constructNewPage(current, next, externalKey, floorPageEncoded, false);
            current = null;
            next = null;
            return newPage;
        }
    }


    /**
     * Helper method for loadPage().
     */
    private Page<K, V> loadPageCacheFloorEntry(Page<K, V> current, K externalKey) {
        while (true) {
            Map.Entry<K, Page<K, V>> cacheEntry = getCache().floorEntry(externalKey);
            K cacheKey = cacheEntry.getKey();
            Page<K, V> cachePage = cacheEntry.getValue();

            assert (cacheKey.equals(cachePage.getFirstKey()));

            /** If the nearest page in cache equals the new page then return. */
            /** If we did not provide a hint then begin with the nearest page in cache. */
            /** If we provided a hint and it was incorrect then do not use the hint. */
            if (cacheKey.equals(externalKey) || current == null || !cacheKey.equals(current.getFirstKey())) {
                if (!cachePage.inTransientState()) {
                    return cachePage;
                }
            }
            /** Else we are using the hint that was provided. */
            else {
                return current;
            }
        }

    }

    /**
     * Helper method for loadPage().
     */
    protected Page<K, V> constructNewPage(Page<K, V> current, Page<K, V> next,
                                          K externalKey, byte[] floorPageEncoded, boolean lock) {

        Page<K, V> newPage = pageFactory.generateEmptyPage(this, externalKey, null);
        newPage.decode(floorPageEncoded);

        assert (newPage.getFirstKey().equals(externalKey));
        assert (compareKeys(current.getFirstKey(), newPage.getFirstKey()) < 0);
        assert (next == null || compareKeys(next.getFirstKey(), newPage.getFirstKey()) > 0);

        Page<K, V> oldPage = getCache().putIfAbsent(externalKey, newPage);
        assert (oldPage == null);

        updateMemoryEstimate(newPage.getMemoryEstimate());
        cacheSize.getAndIncrement();
        numPagesInMemory.getAndIncrement();

        writeUnlockAndNull(current);
        writeUnlockAndNull(next);

        while (shouldEvictPage() || mustEvictPage()) {
            fixedNumberEviction(fixedNumberEvictions);
        }

        if (lock) {
            newPage.writeLock();
        }
        getEvictionQueue().offer(newPage);
        return newPage;
    }


    protected void deletePage(final K targetKey) {
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

                if (!prevPage.getFirstKey().equals(prevKey)) {
                    continue;
                }

                Map.Entry<K, Page<K, V>> currentEntry = getCache().higherEntry(prevKey);

                if (currentEntry == null) {
                    return;
                }

                currentPage = currentEntry.getValue();
                if (useLocks) {
                    currentPage.writeLock();
                }
                if (currentPage.inTransientState()) {
                    continue;
                }
                int compareKeys = compareKeys(targetKey, currentPage.getFirstKey());
                if (compareKeys > 0) {
                    continue;
                } else if (compareKeys == 0 && currentPage.size() == 0) {
                    byte[] verifyPrevKeyEncoded = externalStore.lowerKey(encodedTargetKey);
                    // Test whether the lower key moved while we
                    // were acquiring locks on prevPage and currentPage.
                    if (verifyPrevKeyEncoded == null ||
                            !prevKey.equals(keyCoder.keyDecode(verifyPrevKeyEncoded))) {
                        continue;
                    }
                    externalStore.delete(encodedTargetKey);
                    Page<K, V> prev = getCache().remove(targetKey);
                    assert (prev != null);
                    currentPage.setState(ExternalMode.DELETED);
                    numPagesInMemory.getAndDecrement();
                    numPagesDeleted.getAndIncrement();
                    prevPage.setNextFirstKey(currentPage.getNextFirstKey());
                    prevPage.setState(ExternalMode.DISK_MEMORY_DIRTY);
                }
                return;
            } finally {
                if (useLocks) {
                    writeUnlockAndNull(currentPage);
                    writeUnlockAndNull(prevPage);
                }
            }

        }
    }

    protected V putIntoPage(Page<K, V> page, K key, V value) {
        V prev;
        int offset = binarySearch(page.keys(), key, comparator);

        // An existing (key, value) pair is found.
        if (offset >= 0) {
            page.fetchValue(offset);

            prev = page.values().set(offset, value);
            page.rawValues().set(offset, null);

            updateMemoryCounters(page, key, value, prev);
        } else { // An existing (key, value) pair is not found.
            int position = ~offset;

            page.keys().add(position, key);
            page.values().add(position, value);
            page.rawValues().add(position, null);

            prev = null;

            // updateMemoryCounters must be invoked before incrementing size.
            updateMemoryCounters(page, key, value, null);
            page.setSize(page.size() + 1);
        }
        return prev;
    }

    /**
     * Internal helper method.
     * If the input page is null then do nothing. Otherwise
     * unlock the page. Always return null.
     */
    protected Page<K, V> unlockAndNull(Page<K, V> input, LockMode mode) {
        if (input == null) {
            return null;
        }
        if (useLocks) {
            input.modeUnlock(mode);
        } else {
            input.incrementWriteStamp();
        }
        return null;
    }

    protected Page<K, V> writeUnlockAndNull(Page<K, V> input) {
        return unlockAndNull(input, LockMode.WRITEMODE);
    }

    /**
     * Helper method for {@link #getFirstKey()}.
     */
    protected K firstKeyFastPath() {
        Page<K, V> leftSentinel = getCache().firstEntry().getValue();
        if (useLocks) {
            leftSentinel.readLock();
        }
        try {
            if (leftSentinel.keys() == null) {
                pullPageFromDisk(leftSentinel, useLocks ? LockMode.READMODE : null);
            }

            assert (!leftSentinel.inTransientState());

            if (leftSentinel.size() > 0) {
                return leftSentinel.keys().get(0);
            }
        } finally {
            if (useLocks) {
                leftSentinel.readUnlock();
            }
        }
        return null;
    }


    @Override
    public K getFirstKey() {
        // Fast path: the first key is located in the left sentinel page
        K fastPath = firstKeyFastPath();
        if (fastPath != null) return fastPath;

        Page<K, V> currentPage = getCache().firstEntry().getValue();
        K currentKey = currentPage.getFirstKey();
        byte[] currentKeyEncoded = keyCoder.keyEncode(currentKey);

        if (useLocks) {
            currentPage.writeLock();
        }

        try {
            // Slow path: we load each page from disk searching for the first key.
            while (true) {
                assert (!currentPage.inTransientState());

                if (currentPage.keys() == null) {
                    pullPageFromDisk(currentPage, useLocks ? LockMode.WRITEMODE : null);
                }

                if (currentPage.size() > 0) {
                    return currentPage.keys().get(0);
                }

                byte[] nextKeyEncoded = externalStore.higherKey(currentKeyEncoded);

                if (nextKeyEncoded == null) {
                    return null;
                }

                K nextKey = keyCoder.keyDecode(nextKeyEncoded);
                currentPage = loadPage(nextKey, currentPage);
                currentKey = currentPage.getFirstKey();
                currentKeyEncoded = keyCoder.keyEncode(currentKey);
            }
        } finally {
            if (useLocks) {
                currentPage.writeUnlock();
            }
        }
    }

    /**
     * Helper method for {@link #getLastKey()}.
     */
    protected K lastKeyFastPath() {
        Page<K, V> maxPage = getCache().lastEntry().getValue();
        if (useLocks) {
            maxPage.readLock();
        }
        try {
            if (maxPage.keys() == null) {
                pullPageFromDisk(maxPage, useLocks ? LockMode.READMODE : null);
            }

            if (!maxPage.inTransientState() && maxPage.getNextFirstKey() == null && maxPage.size() > 0) {
                return maxPage.keys().get(maxPage.size() - 1);
            }
        } finally {
            if (useLocks) {
                maxPage.readUnlock();
            }
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
                    // lockmode ignored if we are not using locks
                    pullPageFromDisk(currentPage, LockMode.WRITEMODE);
                }

                if (currentPage.size() > 0) {
                    return currentPage.keys().get(currentPage.size() - 1);
                }

                // This loop is needed to detect concurrent page split operations.
                do {
                    prevKeyEncoded = externalStore.lowerKey(currentKeyEncoded);

                    if (prevKeyEncoded == null) {
                        return null;
                    }

                    prevKey = keyCoder.keyDecode(prevKeyEncoded);

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

    private class PageCacheIterator implements Iterator<Map.Entry<K, V>> {

        Page<K, V> page;
        int position;
        long stamp;
        K prevKey;
        K nextKey;
        V nextValue;

        PageCacheIterator(K from) {
            this.page = locatePage(from, LockMode.READMODE);
            this.prevKey = null;
            this.stamp = -1;

            nextHelper(from, true, false);

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

            Map.Entry<K, V> pair = new PageCacheKeyValue(nextKey, nextValue);

            nextHelper(prevKey, false, true);

            return pair;
        }

        @Override
        public void remove() {
            if (prevKey != null) {
                getCache().remove(prevKey);
            }
        }

        private void nextHelper(K target, boolean inclusive, boolean acquireLock) {
            if (useLocks && acquireLock) {
                page.readLock();
            }

            try {
                assert !useLocks || page.isReadLockedByCurrentThread();

                if (page.keys() == null) {
                    pullPageFromDisk(page, LockMode.READMODE);
                    assert !useLocks || page.isReadLockedByCurrentThread();
                }

                if (page.inTransientState()) {
                    Page<K, V> newPage = locatePage(target, LockMode.READMODE);

                    assert (!newPage.inTransientState());
                    assert (newPage.keys() != null);

                    page = unlockAndNull(page, LockMode.READMODE);
                    page = newPage;
                    assert !useLocks || page.isReadLockedByCurrentThread();
                    stamp = -1;
                }

                if (stamp != page.getWriteStamp()) {
                    position = binarySearch(page.keys(), target, comparator);
                    if (position < 0) {
                        position = ~position;
                    } else if (!inclusive) {
                        position = position + 1;
                    }
                    stamp = page.getWriteStamp();
                } else {
                    position++;
                }

                while (position < page.size() && page.values().get(position) == null
                       && nullRawValue(page.rawValues().get(position))) {
                    position++;
                }


                if (position == page.size() && !moveForward(target, inclusive)) {
                    assert !useLocks || page.isReadLockedByCurrentThread();
                    return;
                }

                page.fetchValue(position);

                nextKey = page.keys().get(position);
                nextValue = page.values().get(position);
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
                byte[] higherKeyEncoded = externalStore.higherKey(keyCoder.keyEncode(page.getFirstKey()));

                if (higherKeyEncoded == null) {
                    nextKey = null;
                    nextValue = null;
                    return false;
                }

                K higherKey = keyCoder.keyDecode(higherKeyEncoded);

                if (useLocks) {
                    page.readUnlock();
                }

                Page<K, V> higherPage = locatePage(higherKey, LockMode.READMODE, true);

                if (higherPage == null) {
                    if (useLocks) {
                        page.readLock();
                    }
                    continue;
                }

                assert (!higherPage.inTransientState());
                assert (higherPage.keys() != null);

                page = higherPage;

                assert (page.keys() != null);

                position = binarySearch(page.keys(), targetKey, comparator);

                if (position < 0) {
                    position = ~position;
                } else if (!inclusive) {
                    position = position + 1;
                }

                while (position < page.size() && page.values().get(position) == null
                        && nullRawValue(page.rawValues().get(position))) {
                    position++;
                }

                if (position < page.size()) {
                    stamp = page.getWriteStamp();
                    return true;
                }
            }
        }
    }


    /**
     * Emit a log message that a page has been detected with a null nextFirstKey
     * and the page is not the largest page in the database.
     *
     * @param repair  if true then repair the page
     * @param counter page number.
     * @param page    contents of the page.
     * @param key     key associated with the page.
     * @param nextKey key associated with the next page.
     */
    private void missingNextFirstKey(final boolean repair, final int counter, Page<K, V> page,
                                     final K key, final K nextKey) {
        log.warn("On page {} the firstKey is {} " +
                        " the length is {} " +
                        " the nextFirstKey is null and the next page" +
                        " is associated with key {}.",
                counter, page.getFirstKey(), page.size(), nextKey);
        if (repair) {
            log.info("Repairing nextFirstKey on page {}.", counter);
            page.setNextFirstKey(nextKey);
            ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
            try {
                byte[] pageEncoded = page.encode(byteBufOutputStream);
                externalStore.put(keyCoder.keyEncode(key), pageEncoded);
            } finally {
                byteBufOutputStream.buffer().release();
            }
        }
    }

    private void repairInvalidKeys(final int counter, Page<K, V> page, final K key, final K nextKey) {
        boolean pageTransfer = false;
        Page<K, V> nextPage = pageFactory.generateEmptyPage(this, nextKey, page.getEncodeType());
        byte[] encodedNextPage = externalStore.get(keyCoder.keyEncode(nextKey));
        nextPage.decode(encodedNextPage);
        for (int i = 0, pos = 0; i < page.size(); i++, pos++) {
            K testKey = page.keys().get(i);
            // if testKey >= nextKey then we need to move the testKey off the current page
            if (compareKeys(testKey, nextKey) >= 0) {
                // non-negative value from binary search indicates the key was found on the next page
                if (binarySearch(nextPage.keys(), testKey, comparator) >= 0) {
                    log.info("Key {} was detected on next page. Deleting from page {}.",
                            pos, counter);
                } else {
                    log.info("Moving key {} on page {}.", pos, counter);
                    page.fetchValue(i);
                    V value = page.values().get(i);
                    putIntoPage(nextPage, testKey, value);
                    pageTransfer = true;
                }
                page.keys().remove(i);
                page.rawValues().remove(i);
                page.values().remove(i);
                page.setSize(page.size() - 1);
                i--;
            }
        }
        ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
        try {
            byte[] pageEncoded = page.encode(byteBufOutputStream);
            externalStore.put(keyCoder.keyEncode(key), pageEncoded);
            if (pageTransfer) {
                encodedNextPage = nextPage.encode(byteBufOutputStream);
                externalStore.put(keyCoder.keyEncode(nextKey), encodedNextPage);
            }
        } finally {
            byteBufOutputStream.buffer().release();
        }

    }

    /**
     * Emit a log message that a page has been detected with an incorrect nextFirstKey
     * and the page is not the largest page in the database.
     *
     * @param repair  if true then repair the page and possibly move entries to the next page
     * @param counter page number.
     * @param page    contents of the page.
     * @param key     key associated with the page.
     * @param nextKey key associated with the next page.
     */
    private void invalidNextFirstKey(final boolean repair, final int counter, Page<K, V> page,
                                     final K key, final K nextKey) {
        int compareTo = compareKeys(page.getNextFirstKey(), nextKey);
        char direction = compareTo > 0 ? '>' : '<';
        log.warn("On page " + counter + " the firstKey is " +
                page.getFirstKey() + " the length is " + page.size() +
                " the nextFirstKey is " + page.getNextFirstKey() +
                " which is " + direction + " the next page is associated with key " + nextKey);
        if (repair) {
            log.info("Repairing nextFirstKey on page {}.", counter);
            page.setNextFirstKey(nextKey);
            repairInvalidKeys(counter, page, key, nextKey);
        }

    }

    public int testIntegrity(boolean repair) {
        int counter = 0;
        int failedPages = 0;
        byte[] encodedKey = externalStore.firstKey();
        byte[] encodedPage = externalStore.get(encodedKey);
        K key = keyCoder.keyDecode(encodedKey);
        while (encodedKey != null) {
            counter++;
            Page<K, V> page = pageFactory.generateEmptyPage(this, key, null);
            byte[] encodedNextKey = externalStore.higherKey(encodedKey);
            if (encodedNextKey != null) {
                page.decode(encodedPage);
                K nextKey = keyCoder.keyDecode(encodedNextKey);
                int numKeys = page.keys().size();
                if (page.getNextFirstKey() == null) {
                    missingNextFirstKey(repair, counter, page, key, nextKey);
                    failedPages++;
                } else if (!page.getNextFirstKey().equals(nextKey)) {
                    invalidNextFirstKey(repair, counter, page, key, nextKey);
                    failedPages++;
                } else if (numKeys > 0 && compareKeys(page.keys().get(numKeys - 1), nextKey) >= 0) {
                    log.warn("On page " + counter + " the firstKey is " +
                            page.getFirstKey() + " the largest key is " + page.keys().get(numKeys - 1) +
                            " the next key is " + nextKey +
                            " which is less than or equal to the largest key.");
                    if (repair) {
                        repairInvalidKeys(counter, page, key, nextKey);
                    }
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
        return repair ? 0 : failedPages;
    }

    private class PageCacheKeyValue implements Map.Entry<K, V> {

        final K key;
        V value;

        public PageCacheKeyValue(K key, V value) {
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
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key
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

    @VisibleForTesting
    public void waitForPageEviction() {

        while (shouldEvictPage()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean pushAllPagesToDiskAssertion() {
        byte[] firstKeyEncoded = externalStore.firstKey();
        K firstKey = keyCoder.keyDecode(firstKeyEncoded);
        return firstKey.equals(negInf);
    }


    @VisibleForTesting
    public void pushAllPagesToDisk() {
        final ByteBufOutputStream byteStream = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
        try {
            for (Page<K, V> page : getEvictionQueue()) {
                if (useLocks) {
                    page.writeLock();
                }
                try {
                    if (!page.inTransientState() && page.keys() != null) {
                        pushPageToDisk(page, byteStream);
                    }
                } finally {
                    if (useLocks) {
                        page.writeUnlock();
                    }
                }
            }
        } finally {
            byteStream.buffer().release();
        }

        assert (pushAllPagesToDiskAssertion());
    }


    protected abstract void doRemove(K key, K end);

    protected abstract V doRemove(K key);

    protected abstract V doPut(K key, V value);


    @Override
    public Iterator<Map.Entry<K, V>> range(K start) {
        return new PageCacheIterator(start);
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

    /**
     * @param start lower bound of range deletion (inclusive)
     * @param end   upper bound of range deletion (exclusive)
     */
    @Override
    public void removeValues(K start, K end) {
        doRemove(start, end);
    }


}

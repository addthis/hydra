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
package com.addthis.hydra.store.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

import java.text.DecimalFormat;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.IteratorClone;
import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.kv.metrics.ExternalPagedStoreMetrics;
import com.addthis.hydra.store.util.MetricsUtil;

import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.jcraft.jzlib.InflaterInputStream;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * TODO should be re-factored to use any KV<byte[],byte[]> store rather than
 * separate ByteStore interface.  this would allow us to implement BDB or
 * any other KV as a KV store and plug it into a paged storage mechanism.
 * <p/>
 * TODO test page deletion when a page becomes empty (for better trash support)
 *
 * @param <K>
 * @param <V>
 */
public class ExternalPagedStore<K extends Comparable<K>, V> extends CachedPagedStore<K, V> {

    private static final boolean collectMetrics = Parameter.boolValue("eps.debug.collect", false);

    private static final Logger log = LoggerFactory.getLogger(ExternalPagedStore.class);
    private static final int gzlevel = Parameter.intValue("eps.gz.level", 1);
    private static final int gztype = Parameter.intValue("eps.gz.type", 1);
    private static final int gzbuf = Parameter.intValue("eps.gz.buffer", 1024);
    private static final int defaultMaxPages = Parameter.intValue("eps.cache.pages", 50);
    private static final int defaultMaxPageEntries = Parameter.intValue("eps.cache.page.entries", 50);
    private static final int defaultEstimateInterval = Parameter.intValue("eps.mem.estimate.interval", 0);
    private static final int memEstimationStrategy = Parameter.intValue("eps.mem.estimate.method", 1);
    private static final int estimateRollMin = Parameter.intValue("eps.mem.estimate.roll.min", 1000);
    private static final int estimateRollFactor = Parameter.intValue("eps.mem.estimate.roll.factor", 100);
    private static final int estimateMissingFactor = Parameter.intValue("eps.mem.estimate.missing.factor", 8);
    private static final boolean trackEncodingByteUsage = Parameter.boolValue("eps.cache.track.encoding", false);
    private static final boolean fixNextFirstKey = Parameter.boolValue("eps.repair.nextfirstkey", false);

    /**
     * used a an absolute delta from maxPages when using that upper bound, otherwise it's treated
     * as a percentage of maxTotalMemory
     */
    private static final int shouldEvictDelta = Parameter.intValue("eps.cache.evict.delta", 20);
    private static final DecimalFormat csm = new DecimalFormat("#,###");
    private static final AtomicLong globalMemoryEstimate = new AtomicLong(0);
    private static long globalMaxTotalMem;
    private static long globalSoftTotalMem;

    private final ExternalPagedStoreMetrics metrics;

    private static final int FLAGS_HAS_ESTIMATES = 1 << 4;

    private static final AtomicInteger scopeGenerator = new AtomicInteger();

    private final String scope = "ExternalPagedStore" + Integer.toString(scopeGenerator.getAndIncrement());

    final Histogram encodeFirstKeySize = trackEncodingByteUsage ?
                                         Metrics.newHistogram(getClass(), "encodeFirstKeySize", scope) :
                                         null;

    final Histogram encodeNextFirstKeySize = trackEncodingByteUsage ?
                                             Metrics.newHistogram(getClass(), "encodeNextFirstKeySize", scope) :
                                             null;

    final Histogram encodeKeySize = trackEncodingByteUsage ?
                                    Metrics.newHistogram(getClass(), "encodeKeySize", scope) :
                                    null;

    final Histogram encodeValueSize = trackEncodingByteUsage ?
                                      Metrics.newHistogram(getClass(), "encodeValueSize", scope) :
                                      null;

    // This metrics counts compressed bytes
    final Histogram encodePageSize = trackEncodingByteUsage ?
                                     Metrics.newHistogram(getClass(), "encodePageSize", scope) :
                                     null;

    final Histogram numberKeysPerPage = trackEncodingByteUsage ?
                                        Metrics.newHistogram(getClass(), "numberKeysPerPage", scope) :
                                        null;
    private final KeyCoder<K, V> keyCoder;

    /** */
    public static interface ByteStore {

        public boolean hasKey(byte[] key);

        public boolean isReadOnly();

        public byte[] firstKey();

        public byte[] lastKey();

        /**
         * delete selected entry and return the lexicographically previous key
         */
        public byte[] delete(byte[] key);

        public void put(byte[] key, byte[] val);

        public byte[] get(byte[] key);

        /**
         * return the least key strictly greater than the given key, or null if there is no such key.
         */
        public byte[] higherKey(byte[] key);

        /**
         * returns the greatest key strictly less than the given key, or null if there is no such key.
         */
        public byte[] lowerKey(byte[] key);

        /**
         * return greatest key less than or equal to 'key'
         */
        public byte[] floorKey(byte[] key);

        /**
         * return (key,value) with greatest key less than or equal to 'key'
         */
        public Map.Entry<byte[], byte[]> floorEntry(byte[] key);

        /**
         * return first entry in db
         */
        public byte[] firstEntry();

        public ClosableIterator<PageEntry> iterator(byte[] start);

        public ClosableIterator<PageEntry> keyIterator(byte[] start);

        public void close();

        /**
         * Close the database.
         *
         * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
         */
        public void close(boolean cleanLog);

        long count();
    }

    /** */
    public static interface PageEntry {

        public byte[] key();

        public byte[] value();
    }

    public static void setKeyDebugging(boolean debug) {
        System.setProperty("eps.keys.debug", debug ? "1" : "0");
    }

    private final int mem_treepage = (int) MemoryCounter.estimateSize(new TreePage(null));
    private final int mem_entry = (int) MemoryCounter.estimateSize(new PageValue((V) null)) + 16;
    private final boolean checkKeyRange = Parameter.boolValue("eps.keys.debug", false);
    private final AtomicLong estimateCounter = new AtomicLong(0);
    private final AtomicLong memoryEstimate;
    private final AtomicLong totalKeys = new AtomicLong(0);
    private final AtomicInteger pageDelete = new AtomicInteger(0);
    private final AtomicInteger pageDeleteHadPrev = new AtomicInteger(0);
    private final AtomicInteger pageDeletePrevCached = new AtomicInteger(0);
    private final AtomicInteger released = new AtomicInteger(0);
    private final AtomicInteger leased = new AtomicInteger(0);
    private final AtomicInteger sampled = new AtomicInteger(0);
    private final AtomicLong numPagesEncoded = new AtomicLong();
    private final AtomicLong numPagesDecoded = new AtomicLong();
    private final AtomicLong numPagesSplit = new AtomicLong();
    private final AtomicBoolean empty;
    private final ByteStore pages;
    private long softTotalMem;
    private long maxTotalMem;
    private long maxPageMem;
    private int estimateInterval;
    private int maxPageSize;
    private int maxPages;

    private Thread memWatcher;

    public ExternalPagedStore(KeyCoder<K, V> keyCoder, ByteStore pages) {
        this(keyCoder, pages, 0, 0);
    }

    public ExternalPagedStore(KeyCoder<K, V> keyCoder, ByteStore pages, int maxPageSize, int maxPages) {
        this.keyCoder = keyCoder;
        this.pages = pages;
        this.maxPages = maxPages;
        this.maxPageSize = maxPageSize;
        this.estimateInterval = defaultEstimateInterval;
        this.empty = new AtomicBoolean(pages.firstKey() == null);
        metrics = collectMetrics ? new ExternalPagedStoreMetrics() : null;

        log.info("[init] ro=" + isReadOnly() + " maxPageSize=" + maxPageSize + " maxPages=" + maxPages + " gztype=" + gztype + " gzlevel=" + gzlevel + " gzbuf=" + gzbuf + " mem[page=" + mem_treepage + ",ent=" + mem_entry + "]");
        if (isReadOnly()) {
            memoryEstimate = globalMemoryEstimate;
        } else {
            memoryEstimate = new AtomicLong(0);
            startCacheCleaner();
        }
        if (Parameter.intValue("eps.mem.debug", 0) > 0) {
            startMemWatcher();
        }
    }

    @Override
    public boolean isReadOnly() {
        return pages.isReadOnly();
    }

    @Override
    public String toString() {
        return "EPS:keys=" + totalKeys + ",mem=" + memoryEstimate + ",cache=" + getCachedEntryCount() + ",cachex=" + maxTotalMem + ":" + maxPages + ",pagex=" + maxPageMem + ":" + maxPageSize;
    }

    @Override
    public void setMaxPages(int maxPages) {
        this.maxPages = maxPages;
    }

    @Override
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
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
    public void setMaxPageMem(long maxPageMem) {
        this.maxPageMem = maxPageMem;
    }

    @Override
    public void setMemEstimateInterval(int interval) {
        this.estimateInterval = interval;
    }

    public long getMemoryEstimate() {
        return memoryEstimate.get() + (getCachedEntryCount() * mem_treepage);
    }

    private void updateMemoryEstimate(int delta) {
        memoryEstimate.addAndGet(delta);
    }

    private final void updateHistogram(Histogram histogram, int value) {
        /**
         *  The JIT compiler should be smart enough to eliminate this code
         *  when {@link ExternalPagedStore.trackEncodingByteUsage} is false.
         */
        if (ExternalPagedStore.trackEncodingByteUsage) {
            histogram.update(value);
        }
    }

    private byte[] pageEncode(TreePage page) {
        numPagesEncoded.getAndIncrement();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputStream os = out;
            out.write(gztype | FLAGS_HAS_ESTIMATES);
            switch (gztype) {
                case 0:
                    break;
                case 1:
                    os = new DeflaterOutputStream(out, new Deflater(gzlevel));
                    break;
                case 2:
                    os = new GZOut(out, gzbuf, gzlevel);
                    break;
                case 3:
                    os = new LZFOutputStream(out);
                    break;
                case 4:
                    os = new SnappyOutputStream(out);
                    break;
                default:
                    throw new RuntimeException("invalid gztype: " + gztype);
            }
            Bytes.writeLength(page.map.size(), os);

            byte[] firstKeyEncoded = keyCoder.keyEncode(page.getFirstKey());
            byte[] nextFirstKeyEncoded = keyCoder.keyEncode(page.getNextFirstKey());

            updateHistogram(encodeFirstKeySize, firstKeyEncoded.length);
            updateHistogram(encodeNextFirstKeySize, nextFirstKeyEncoded.length);

            Bytes.writeBytes(firstKeyEncoded, os);
            Bytes.writeBytes(nextFirstKeyEncoded, os);


            for (Entry<K, PageValue> e : page.map.entrySet()) {
                byte[] nextKey = keyCoder.keyEncode(e.getKey());
                byte[] nextValue = e.getValue().raw();

                updateHistogram(encodeKeySize, nextKey.length);
                updateHistogram(encodeValueSize, nextValue.length);

                Bytes.writeBytes(nextKey, os);
                Bytes.writeBytes(nextValue, os);
            }
            Bytes.writeLength((page.estimateTotal > 0 ? page.estimateTotal : 1), os);
            Bytes.writeLength((page.estimates > 0 ? page.estimates : 1), os);
            switch (gztype) {
                case 1:
                    ((DeflaterOutputStream) os).finish();
                    break;
                case 2:
                    ((GZOut) os).finish();
                    break;
            }
            os.flush();
            os.close();
            byte[] bytesOut = out.toByteArray();
            if (log.isDebugEnabled()) log.debug("encoded " + bytesOut.length + " bytes to " + page);
            updateHistogram(encodePageSize, bytesOut.length);
            updateHistogram(numberKeysPerPage, page.map.size());
            return bytesOut;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private TreePage pageDecode(byte[] page) {
        numPagesDecoded.getAndIncrement();
        try {
            InputStream in = new ByteArrayInputStream(page);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            boolean hasEstimates = (flags & FLAGS_HAS_ESTIMATES) != 0;
            switch (gztype) {
                case 1:
                    in = new InflaterInputStream(in);
                    break;
                case 2:
                    in = new GZIPInputStream(in);
                    break;
                case 3:
                    in = new LZFInputStream(in);
                    break;
                case 4:
                    in = new SnappyInputStream(in);
                    break;
            }
            int entries = (int) Bytes.readLength(in);
            int count = entries;
            K firstKey = keyCoder.keyDecode(Bytes.readBytes(in));
            K nextFirstKey = keyCoder.keyDecode(Bytes.readBytes(in));
            TreePage decode = new TreePage(firstKey).setNextFirstKey(nextFirstKey);
            int bytes = 0;
            while (count-- > 0) {
                byte kb[] = Bytes.readBytes(in);
                byte vb[] = Bytes.readBytes(in);
                bytes += kb.length + vb.length;
                K key = keyCoder.keyDecode(kb);
                decode.map.put(key, new PageValue(vb));
            }
            if (maxTotalMem > 0) {
                if (hasEstimates) {
                    decode.setAverage((int) Bytes.readLength(in), (int) Bytes.readLength(in));
                } else {
                    /** use a pessimistic/conservative byte/entry estimate */
                    decode.setAverage(bytes * estimateMissingFactor, entries);
                }
            }
            in.close();
            if (log.isDebugEnabled()) log.debug("decoded " + decode);
            return decode;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * wrapper to allow deferred decoding
     */
    private final class PageValue {

        private V value;
        private byte[] raw;

        PageValue(V value) {
            this.value = value;
        }

        PageValue(byte[] raw) {
            this.raw = raw;
        }

        @Override
        public String toString() {
            return "PV:" + (value != null ? value : raw != null ? "{raw:" + raw.length + "}" : "null");
        }

        public V value() {
            if (value == null && raw != null) {
                value = keyCoder.valueDecode(raw);
                raw = null;
            }
            return value;
        }

        public byte[] raw() {
            if (raw == null) {
                raw = keyCoder.valueEncode(value);
            }
            return raw;
        }
    }

    /** */
    private final class TreePage implements KeyValuePage<K, V>, Comparator<K> {

        private final AtomicInteger pins = new AtomicInteger(0);
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final TreeMap<K, PageValue> map;
        private final K firstKey;
        private int estimateTotal;
        private int estimates;
        private int avgEntrySize;
        private K nextFirstKey;
        private volatile boolean dirty;

        TreePage(K firstKey) {
            this.firstKey = firstKey;
            this.map = new TreeMap<K, PageValue>(this);
        }

        TreePage setNextFirstKey(K nextFirstKey) {
            this.nextFirstKey = nextFirstKey;
            this.dirty = true;
            return this;
        }

        int estimatedMem() {
            return (avgEntrySize + mem_entry) * map.size();
        }

        private void updateAverage(K key, PageValue val, int count) {
            if (maxTotalMem + maxPageMem == 0) {
                return;
            }
            long next = estimateCounter.incrementAndGet();
            if (map.size() == 0 || (estimateInterval <= 0 && estimates > 0 && next % estimates == 0) ||
                (estimateInterval > 0 && next % estimateInterval == 0)) {
                switch (memEstimationStrategy) {
                    case 0:
                        /** use encoded byte size as crude proxy for mem size */
                        updateAverage((keyCoder.keyEncode(key).length + val.raw().length), count);
                        break;
                    case 1:
                        /** walk objects and estimate.  possibly slower and not demonstrably more accurate */
                        updateAverage((int) (MemoryCounter.estimateSize(key) + MemoryCounter.estimateSize(val)), count);
                        break;
                    default:
                        throw new RuntimeException("invalid sample strategy: " + memEstimationStrategy);
                }
                sampled.incrementAndGet();
            }
        }

        private void updateAverage(int size, int count) {
            if (estimates < Math.min(estimateRollMin, map.size() * estimateRollFactor)) {
                estimates += count;
                estimateTotal += (size * count);
                avgEntrySize = estimateTotal / estimates;
            } else {
                estimates = 1;
                estimateTotal = avgEntrySize;
            }
        }

        private void setAverage(int total, int count) {
            avgEntrySize = count > 0 ? total / count : 0;
            estimates = count;
            estimateTotal = total;
        }

        @Override
        public String toString() {
            return "tp[" + map.size() + "," + firstKey + "," + nextFirstKey + "]";
        }

        @Override
        public boolean reValidate() {
            return !pages.isReadOnly() && splitCheck();
        }

        boolean splitCheck() {
            if (isPinned()) {
                return false;
            }
            if (maxPageMem > 0 && estimatedMem() > maxPageMem && map.size() > 1) {
                split();
                return true;
            } else if (maxPageSize > 0) {
                if (map.size() > maxPageSize) {
                    split();
                    return true;
                }
            } else if (map.size() > defaultMaxPageEntries) {
                split();
                return true;
            }
            return false;
        }

        void split() {
            numPagesSplit.getAndIncrement();
            TreePage newPage = null;
            int skip = map.size() / 2;
            for (Iterator<Entry<K, PageValue>> iter = map.entrySet().iterator(); iter.hasNext();) {
                Entry<K, PageValue> next = iter.next();
                if (skip-- > 0) {
                    continue;
                }
                if (newPage == null) {
                    newPage = new TreePage(next.getKey()).setNextFirstKey(getNextFirstKey());
                    setNextFirstKey(next.getKey());
                    /**
                     * the reason this works here vs after the split is complete:
                     * this puts a key in the backing store so that ranges still work.
                     * ranges always check the hot cache first before decoding, so
                     * this shell is never decoded.  once the complete page is
                     * flushed from cache, it is written in full.
                     */
                    pages.put(keyCoder.keyEncode(newPage.getFirstKey()), pageEncode(newPage));
                    putPageIntoCache(newPage);
                }
                newPage.map.put(next.getKey(), next.getValue());
                iter.remove();
            }
            if (maxTotalMem > 0) {
                newPage.avgEntrySize = avgEntrySize;
                newPage.estimates = estimates;
                newPage.estimateTotal = estimateTotal;
            }
            dirty = true;
            if (log.isDebugEnabled()) log.debug("split post map=" + map + ", new=" + newPage.map + ", pages=" + pages);
        }

        void checkKey(K key) {
            if (!checkKeyRange) {
                return;
            }
            if (key.compareTo(firstKey) < 0 || (nextFirstKey != null && key.compareTo(nextFirstKey) >= 0)) {
                throw new RuntimeException("getPut out of range " + key + " compared to " + firstKey + " - " + nextFirstKey);
            }
        }

        @Override
        public boolean containsKey(K key) {
            lock.readLock().lock();
            try {
                return map.containsKey(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getLastKey() {
            lock.readLock().lock();
            try {
                return map.lastKey();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public V getValue(K key) {
            lock.readLock().lock();
            checkKey(key);
            try {
//              int memest = estimatedMem();
                PageValue pv = map.get(key);
                if (collectMetrics) {
                    metrics.updateGetValue(pv);
                }
                if (pv != null) {
                    V val = pv.value();
//                  updateAverage(key, pv, 1);
//                  updateMemoryEstimate(estimatedMem() - memest);
                    return val;
                } else {
                    return null;
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public V getPutValue(K key, V val) {
            lock.writeLock().lock();
            checkKey(key);
            try {
                int memest = estimatedMem();
                PageValue newVal = new PageValue(val);
                PageValue old = map.put(key, newVal);
                /** for memory estimation, the replacement gets 2x weighting */
                if (old == null) {
                    totalKeys.incrementAndGet();
                    updateAverage(key, newVal, 1);
                } else {
                    updateAverage(key, newVal, 2);
                }
                dirty = true;
                updateMemoryEstimate(estimatedMem() - memest);
                return old != null ? old.value() : null;
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public V getRemoveValue(K key) {
            lock.writeLock().lock();
            checkKey(key);
            try {
                int memest = estimatedMem();
                PageValue old = map.remove(key);
                if (log.isDebugEnabled()) log.debug("eps.remove " + key + "=" + old + " from " + map);
                if (old != null) {
                    totalKeys.decrementAndGet();
                    dirty = true;
                    updateMemoryEstimate(estimatedMem() - memest);
                }
                return old != null ? old.value() : null;
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void putValue(K key, V val) {
            lock.writeLock().lock();
            checkKey(key);
            try {
                int memest = estimatedMem();
                PageValue newVal = new PageValue(val);
                /** for memory estimation, the replacement gets 2x weighting */
                if (map.put(key, newVal) == null) {
                    totalKeys.incrementAndGet();
                    updateAverage(key, newVal, 1);
                } else {
                    updateAverage(key, newVal, 2);
                }
                updateMemoryEstimate(estimatedMem() - memest);
                dirty = true;
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void removeValues(K start, K end, boolean inclusive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeValue(K key) {
            lock.writeLock().lock();
            checkKey(key);
            try {
                int memest = estimatedMem();
                PageValue old = map.remove(key);
                if (old != null) {
                    if (log.isDebugEnabled()) log.debug("eps.removed " + key + " from " + map);
                    totalKeys.decrementAndGet();
                    dirty = true;
                    updateMemoryEstimate(estimatedMem() - memest);
                } else {
                    if (log.isDebugEnabled()) log.debug("eps.removed failed " + key + " from " + map);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public Iterator<Entry<K, V>> range(K start, boolean inclusive) {
            lock.readLock().lock();
            try {
                SortedMap<K, PageValue> tailMap = start != null ? map.tailMap(start, inclusive) : map;
                if (log.isDebugEnabled()) log.debug("range start=" + start + " tailMap=" + tailMap + " map=" + map);
                return new TreePageIterator(tailMap);
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public boolean mayContainKey(K key) {
            K first = firstKey;
            K last = getNextFirstKey();
            return first.compareTo(key) <= 0 && (last == null || key.compareTo(last) < 0);
        }

        @Override
        public K getNextFirstKey() {
            return nextFirstKey;
        }

        @Override
        public int compareKeys(K k1, K k2) {
            return ExternalPagedStore.this.compareKeys(k1, k2);
        }

        @Override
        public int compare(K o1, K o2) {
            return compareKeys(o1, o2);
        }

        @Override
        public PagePin pin() {
            return new Pin();
        }

        @Override
        public boolean isPinned() {
            return pins.get() > 0;
        }

        private class Pin implements PagePin {

            private int pin = 1;

            {
                pins.incrementAndGet();
            }

            @Override
            public void release() throws IllegalStateException {
                if (pin-- < 0 || pins.decrementAndGet() < 0) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    /**
     * allows iterators to preserve deferred decoding
     */
    private final class TreePageIterator implements Iterator<Entry<K, V>> {

        private final IteratorClone<Entry<K, PageValue>> iter;

        public TreePageIterator(SortedMap<K, PageValue> tailMap) {
            iter = new IteratorClone<Entry<K, PageValue>>(tailMap.entrySet().iterator(), tailMap.size());
        }

        @Override
        public String toString() {
            return "TPI:" + iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            return new Entry<K, V>() {
                private final Entry<K, PageValue> entry = iter.next();

                @Override
                public K getKey() {
                    return entry.getKey();
                }

                @Override
                public V getValue() {
                    return entry.getValue().value();
                }

                @Override
                public V setValue(V value) {
                    PageValue old = entry.setValue(new PageValue(value));
                    return old != null ? old.value() : null;
                }
            };
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    @Override
    public boolean containsKey(K key) {
        return pages.hasKey(keyCoder.keyEncode(key));
    }

    @Override
    public K getFirstKey() {
        return keyCoder.keyDecode(pages.firstKey());
    }

    @Override
    public K getLastKey() {
        return keyCoder.keyDecode(pages.lastKey());
    }

    @Override
    public KeyValuePage<K, V> fetchPage(K key) {
        if (empty.compareAndSet(true, false)) {
            TreePage p = new TreePage(key);
            pages.put(keyCoder.keyEncode(key), pageEncode(p));
            if (log.isDebugEnabled()) log.debug("fetchPage.first " + key + " = " + p + " init " + pages);
            return p;
        }
        byte[] k = keyCoder.keyEncode(key);

        Map.Entry<byte[], byte[]> floorEntry = pages.floorEntry(k);
        byte[] e = (floorEntry != null) ? floorEntry.getValue() : null;
        if (e != null) {
            KeyValuePage<K, V> page = pageDecode(e);
            if (log.isDebugEnabled()) log.debug("fetchPage.decode " + key + " from " + e.length + " bytes = " + page);
            return page;
        }
        /** new first page */
        K firstKey = getFirstKey();
        TreePage firstPage = (TreePage) getCachedPage(firstKey);
        /** first page not in cache? cache it */
        if (firstPage == null) {
            firstPage = pageDecode(pages.firstEntry());
        }
        /** new first page points to previous first page as next page */
        TreePage p = new TreePage(key).setNextFirstKey(firstPage.getFirstKey());
        pages.put(k, pageEncode(p));
        if (log.isDebugEnabled()) log.debug("fetchPage.create " + key + " = " + p + " in " + pages);
        return p;
    }

    @Override
    public void leasePage(KeyValuePage<K, V> page) {
        TreePage tp = (TreePage) page;
        totalKeys.addAndGet(tp.map.size());
        leased.incrementAndGet();
        if (maxTotalMem > 0) {
            updateMemoryEstimate(tp.estimatedMem());
        }
    }

    @Override
    public void releasePage(KeyValuePage<K, V> page) {
        if (log.isDebugEnabled()) log.debug("releasePage " + page);
        if (page.getClass() != TreePage.class) {
            throw new IllegalArgumentException();
        }
        TreePage tp = (TreePage) page;
        totalKeys.addAndGet(-tp.map.size());
        released.incrementAndGet();
        if (maxTotalMem > 0) {
            updateMemoryEstimate(-tp.estimatedMem());
        }
        if (!pages.isReadOnly()) {
            if (tp.dirty) {
                /** delete empty pages */
                if (tp.map.size() == 0) {
                    pageDelete.incrementAndGet();
                    /** update previous page's nextFirstKey if there is a previous pages */
                    byte prev[] = pages.delete(keyCoder.keyEncode(page.getFirstKey()));
                    if (log.isDebugEnabled()) log.debug("deletePage (empty) " + page + " prev=" + prev);
                    if (prev != null) {
                        pageDeleteHadPrev.incrementAndGet();
                        K prevKey = keyCoder.keyDecode(prev);
                        KeyValuePage<K, V> prevPage = getCachedPage(prevKey);
                        /** previous page not in cache? cache it */
                        if (prevPage == null) {
                            if (log.isDebugEnabled()) log.debug("deletePage (prev) no cache for " + prevKey);
                            Map.Entry<byte[], byte[]> floorEntry = pages.floorEntry(prev);
                            byte[] prevPageRaw = (floorEntry != null) ? floorEntry.getValue() : null;
                            prevPage = pageDecode(prevPageRaw);
                            putPageIntoCache(prevPage);
                        } else {
                            pageDeletePrevCached.incrementAndGet();
                        }
                        if (prevPage == null) {
                            throw new RuntimeException("missing page for " + prevKey);
                        }
                        K prevNextFirstKey = prevPage.getNextFirstKey();
                        K currentFirstKey = page.getFirstKey();
                        if (!prevNextFirstKey.equals(currentFirstKey)) {
                            String msg = "previous page next pointer invalid " + prevNextFirstKey + " != " + currentFirstKey;
                            if (fixNextFirstKey) {
                                log.warn(msg);
                                byte[] higherKeyEncoded = pages.higherKey(keyCoder.keyEncode(currentFirstKey));
                                if (higherKeyEncoded == null) {
                                    throw new RuntimeException("New next first key is null value.");
                                }
                                K higherKey = keyCoder.keyDecode(higherKeyEncoded);
                                log.warn("previous page " + prevKey + " setting nextFirstKey to " + higherKey);
                                tp = (TreePage) prevPage;
                                tp.setNextFirstKey(higherKey);
                            } else {
                                throw new RuntimeException(msg);
                            }
                        } else {
                            tp = (TreePage) prevPage;
                            tp.setNextFirstKey(page.getNextFirstKey());
                        }
                    }
                } else {
                    pages.put(keyCoder.keyEncode(page.getFirstKey()), pageEncode((TreePage) page));
                }
            } else if (log.isDebugEnabled()) {
                log.debug("releasePage skip clean page " + page);
            }
        }
    }

    @Override
    public Iterator<Entry<K, KeyValuePage<K, V>>> getPageIterator(final K start) {
        return new PageIterator(pages.iterator(start != null ? keyCoder.keyEncode(start) : null));
    }

    @Override
    public boolean shouldEvictPage() {
        if (maxTotalMem > 0) {
            return getMemoryEstimate() > softTotalMem && getCachedEntryCount() > 5;
        } else if (maxPages > 0) {
            return getCachedEntryCount() >= Math.max(maxPages - shouldEvictDelta, 5);
        } else {
            return getCachedEntryCount() >= Math.max(defaultMaxPages - shouldEvictDelta, 5);
        }
    }

    @Override
    public boolean mustEvictPage() {
        if (maxTotalMem > 0) {
            return getMemoryEstimate() > maxTotalMem && getCachedEntryCount() > 5;
        } else if (maxPages > 0) {
            return getCachedEntryCount() > maxPages;
        } else {
            return getCachedEntryCount() > defaultMaxPages;
        }
    }

    @Override
    public void close() {
        close(false, CloseOperation.NONE);
    }

    /**
     * Close the page store.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     * @return status code. A status code of 0 indicates success.
     **/
    public int close(boolean cleanLog, CloseOperation operation) {
        super.close();
        pages.close(cleanLog);
        stopMemWathcher();
        log.info("pages: encoded=" + numPagesEncoded.get() +
                 " decoded=" + numPagesDecoded.get() +
                 " split=" + numPagesSplit.get());
        if (trackEncodingByteUsage) {
            log.info(MetricsUtil.histogramToString("encodeFirstKeySize", encodeFirstKeySize));
            log.info(MetricsUtil.histogramToString("encodeNextFirstKeySize", encodeNextFirstKeySize));
            log.info(MetricsUtil.histogramToString("encodeKeySize", encodeKeySize));
            log.info(MetricsUtil.histogramToString("encodeValueSize", encodeValueSize));
            log.info(MetricsUtil.histogramToString("encodePageSize (final)", encodePageSize));
            log.info(MetricsUtil.histogramToString("numberKeysPerPage", numberKeysPerPage));
        }
        return 0;
    }

    /** */
    private final class PageIterator implements ClosableIterator<Entry<K, KeyValuePage<K, V>>> {

        private final ClosableIterator<PageEntry> iter;

        PageIterator(ClosableIterator<PageEntry> iter) {
            this.iter = iter;
        }

        @Override
        public String toString() {
            return "PI:" + iter;
        }

        @Override
        public void close() {
            iter.close();
        }

        @Override
        public boolean hasNext() {
            if (log.isDebugEnabled()) log.debug("PI:hasNext=" + iter.hasNext());
            return iter.hasNext();
        }

        @Override
        public Entry<K, KeyValuePage<K, V>> next() {
            return new Entry<K, KeyValuePage<K, V>>() {
                final PageEntry next = iter.next();
                final K key = keyCoder.keyDecode(next.key());

                @Override
                public String toString() {
                    return "KV:" + key + "=" + next;
                }

                @Override
                public K getKey() {
                    return key;
                }

                @Override
                public KeyValuePage<K, V> getValue() {
                    KeyValuePage<K, V> page = getCachedPage(key);
                    if (page == null) {
                        page = pageDecode(next.value());
                    }
                    return page;
                }

                @Override
                public KeyValuePage<K, V> setValue(KeyValuePage<K, V> value) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int compareKeys(K k1, K k2) {
        return k1.compareTo(k2);
    }

    private void stopMemWathcher() {
        if (memWatcher != null) {
            memWatcher.interrupt();
        }
    }

    private void startMemWatcher() {
        if (memWatcher != null) {
            return;
        }
        memWatcher = new Thread() {
            {
                setDaemon(true);
                setName("MemWatcher");
                start();
            }

            public void run() {
                try {
                    _run();
                } finally {
                    memWatcher = null;
                }
            }

            private void _run() {
                int sleep = Parameter.intValue("eps.mem.debug", 0);
                while (true) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        return;
                    }
                    lock.writeLock().lock();
                    try {
                        memReport();
                    } catch (Error e)  {
                        log.warn("", e);
                        throw e;
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            }
        };
    }

    protected void memReport() {
        long sumpage = (getCachedEntryCount() * mem_treepage);
        long min = 0;
        long max = 0;
        int keys = 0;
        // TODO:  move to common package
//      KeyHistogram sizeHisto = new KeyHistogram().setScale(2).init();
        for (KeyValuePage<K, V> page : cache.values()) {
            TreePage tp = (TreePage) page;
            long mem = tp.estimatedMem();
//          sizeHisto.update(0, mem / 1024);
            max = Math.max(max, mem);
            min = min > 0 ? Math.min(min, mem) : mem;
            sumpage += mem;
            keys++;
        }
        int pagein = leased.getAndSet(0);
        int pageout = released.getAndSet(0);
        int requests = pageGets.getAndSet(0);
        int samples = sampled.getAndSet(0);
        int pageDeletes = pageDelete.getAndSet(0);
        int pageDeletesHadPrev = pageDeleteHadPrev.getAndSet(0);
        int pageDeletesPrevCached = pageDeletePrevCached.getAndSet(0);
        if (pagein + pageout == 0 && (requests == 0 || getCachedEntryCount() <= 2)) {
            return;
        }
        log.info(
                "mem=[err=" + csm.format(sumpage - getMemoryEstimate()) + " est=" + csm.format(getMemoryEstimate()) + " sum=" + csm.format(sumpage) + " max=" + csm.format(max) + " min=" + csm.format(min) + " avg=" + csm.format(keys > 0 ? sumpage / keys : 0) + "]" +
                " memX=" + csm.format(maxTotalMem) +
                " pMemX=" + csm.format(maxPageMem) +
                " pages[num=" + csm.format(getCachedEntryCount()) + " in=" + pagein + " out=" + pageout + " del=" + pageDeletes + ";" + pageDeletesHadPrev + ";" + pageDeletesPrevCached + "]" +
                " keys=" + totalKeys +
                " reqs=" + requests +
                " samp=" + samples +
                " srate=" + (samples > 0 ? requests / samples : 0)
//              " histo=" + sizeHisto.getSortedHistogram()
        );
    }
}

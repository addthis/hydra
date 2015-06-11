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
import java.io.DataInputStream;
import java.io.InputStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.db.IReadWeighable;
import com.addthis.hydra.store.db.ReadDBKeyCoder;
import com.addthis.hydra.store.kv.metrics.ExternalPagedStoreMetrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import com.jcraft.jzlib.InflaterInputStream;
import com.ning.compress.lzf.LZFInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

/**
 * read only caching page store intended to play nice with the query system
 * <p/>
 * interacts with an external k-(page of v) store which presumably keeps pages (grouped lists)
 * of the type v given to us
 * <p/>
 * performs three functions:
 * exposes methods to get individual vs (eg tree nodes) from the backing store
 * provides iterators to allow for efficient traversal of the store by individual k/v
 * caches pages to make both kinds of access more efficient for repeated reads
 * <p/>
 * TODO:
 * toString
 * <p/>
 * support reading of either pages or efficient 'page size 1' (individal nodes) from the backing store
 * to support mixing the two if desired (eg a tree branch optimized for random access)
 *
 * @param <K> - key for individual values, also used for pages
 * @param <V> - type of object stored (the backing store will have type Page<V>)
 */
public class ReadExternalPagedStore<K extends Comparable<K>, V extends IReadWeighable & BytesCodable> {

    private static final boolean collectMetricsParameter = Parameter.boolValue("eps.debug.collect", false);
    private static final Logger log = LoggerFactory.getLogger(ReadExternalPagedStore.class);

    private final boolean checkKeyRange = Parameter.boolValue("eps.keys.debug", false);

    private final ExternalPagedStoreMetrics metrics;

    private final boolean collectMetrics;

    protected static final int TYPE_BIT_OFFSET = 5;

    /**
     * guava loading cache for storing pages. Get method takes the exact page key, so finding the
     * page key must be done first.
     */
    private final LoadingCache<K, TreePage> loadingPageCache;

    //backing byte store
    private final ByteStore pages;

    final KeyCoder<K, V> keyCoder;

    public ReadExternalPagedStore(KeyCoder<K, V> keyCoder, final ByteStore pages,
                                  int maxSize, int maxWeight) {
        this(keyCoder, pages, maxSize, maxWeight, false);
    }

    public ReadExternalPagedStore(final KeyCoder<K, V> keyCoder, final ByteStore pages,
                                  int maxSize, int maxWeight, boolean collect) {
        this.keyCoder = keyCoder;
        this.pages = pages;
        log.info("[init] maxSize=" + maxSize + " maxWeight=" + maxWeight);

        collectMetrics = collectMetricsParameter || collect;

        metrics = collectMetrics ? new ExternalPagedStoreMetrics() : null;

        // Prefer evicting on weight instead of page count
        if (maxWeight != 0) {
            loadingPageCache = CacheBuilder.newBuilder()
                    .weigher(new Weigher<K, TreePage>() {
                        @Override
                        public int weigh(K key, TreePage value) {
                            return value.originalByteSize;
                        }
                    })
                    .maximumWeight(maxWeight)
                    .build(
                            new CacheLoader<K, TreePage>() {
                                public TreePage load(K key) throws Exception {
                                    byte[] page = pages.get(keyCoder.keyEncode(key));
                                    if (page != null) {
                                        return pageDecode(page);
                                    } else {
                                        throw new ExecutionException("Source did not have page",
                                                                     new NullPointerException());
                                    }
                                }
                            });
        } else {
            loadingPageCache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .build(
                            new CacheLoader<K, TreePage>() {
                                public TreePage load(K key) throws Exception {
                                    byte[] page = pages.get(keyCoder.keyEncode(key));
                                    if (page != null) {
                                        return pageDecode(page);
                                    } else {
                                        throw new ExecutionException("Source did not have page",
                                                                     new NullPointerException());
                                    }
                                }
                            });
        }
    }

    public ReadDBKeyCoder<V> getKeyCoder() {
        return (ReadDBKeyCoder) keyCoder;
    }

    public K getFirstKey() {
        return keyCoder.keyDecode(pages.firstKey());
    }

    public byte[] getPageKeyForKey(K key) {
        byte[] byteKey = keyCoder.keyEncode(key);
        return pages.floorKey(byteKey);
    }

    public KeyValuePage<K, V> getOrLoadPageForKey(K key) {
        K pageKey = keyCoder.keyDecode(getPageKeyForKey(key));
        if (pageKey != null) {
            try {
                return loadingPageCache.get(pageKey);
            } catch (ExecutionException e) {
            }
        }
        return null;
    }

    /**
     * TODO: Might as well store TreePage keys as undecoded bytes if we only use this method?
     */
    public V getValue(K key) {
        KeyValuePage<K, V> page = getOrLoadPageForKey(key);
        V value = page.getValue(key);
        if (collectMetrics) {
            metrics.updateGetValue(value);
        }
        return value;
    }

    public void close() {
        pages.close();
    }

    //decode pages. Called on the bytes returned by store.get()
    private TreePage pageDecode(byte[] page) {
        try {
            InputStream in = new ByteArrayInputStream(page);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            int pageType = flags >>> TYPE_BIT_OFFSET;
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
            PageEncodeType pageEncodeType;
            DataInputStream dis = null;
            switch (pageType) {
                case 0:
                    pageEncodeType = PageEncodeType.LEGACY;
                    break;
                case 1:
                    pageEncodeType = PageEncodeType.SPARSE;
                    dis = new DataInputStream(in);
                    break;
                case 2:
                    pageEncodeType = PageEncodeType.LONGIDS;
                    dis = new DataInputStream(in);
                    break;
                default:
                    throw new IllegalStateException("unknown page type " + pageType);
            }
            TreePage decode;
            int entries = pageEncodeType.readInt(in, dis);
            if (collectMetrics) {
                metrics.updatePageSize(entries);
            }
            byte[] firstKeyBytes = pageEncodeType.readBytes(in, dis);
            K firstKey = keyCoder.keyDecode(firstKeyBytes);
            byte[] nextFirstKeyBytes = pageEncodeType.nextFirstKey(in, dis);
            K nextFirstKey = keyCoder.keyDecode(nextFirstKeyBytes);
            decode = new TreePage(firstKey).setNextFirstKey(nextFirstKey);
            decode.originalByteSize = 4 + firstKeyBytes.length;
            if (nextFirstKeyBytes != null) {
                decode.originalByteSize += nextFirstKeyBytes.length;
            }

            for (int i = 0; i < entries; i++) {
                byte[] kb = pageEncodeType.readBytes(in, dis);
                decode.originalByteSize += kb.length;
                byte[] vb = pageEncodeType.readBytes(in, dis);
                decode.originalByteSize += vb.length;
                K key = keyCoder.keyDecode(kb, firstKey, pageEncodeType);
                decode.map.put(key, new PageValue(vb, pageEncodeType));
            }

            //ignoring memory data
            in.close();
            log.debug("decoded {}", decode);

            return decode;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * wrapper around an individual (non-paged) value V that allows for selective
     * decoding of tree nodes from a page. Pages start off with a bunch of these.
     * <p/>
     * Does some crazy concurrency things. They might not be a good idea? Kind of cool though.
     */
    private final class PageValue {

        private V value;
        private byte[] raw;
        private volatile V realValue;
        private PageEncodeType encodeType;

        PageValue(byte[] raw, PageEncodeType encodeType) {
            this.raw = raw;
            this.encodeType = encodeType;
        }

        @Override
        public String toString() {
            return "PV:" + (value != null ? value : raw != null ? "{raw:" + raw.length + "}" : "null");
        }

        public V value() {
            if (value == null) {
                byte[] r = raw;
                if (realValue != null) {
                    value = realValue;
                } else if (r != null) {
                    realValue = keyCoder.valueDecode(r, encodeType);
                    value = realValue;
                    raw = null;
                }
            }
            return value;
        }
    }

    /**
     * Implementation of a page. Is constructed by pageDecode. See var comments
     */
    private final class TreePage implements KeyValuePage<K, V>, Comparator<K> {

        //An ordered mapping of K (individual/non-paged keys) to PageValues (decode-deferring wrappers for V)
        private final TreeMap<K, PageValue> map;

        //The first key in the map. also used as the key for this page in backing store
        private final K firstKey;

        //The first of the next page (apparently). should also be the key for the next page in the backing store (?)
        //If this is always correct -> we can use our own key iterator without relying on one from backing store
        private K nextFirstKey;

        private int originalByteSize;

        TreePage(K firstKey) {
            this.firstKey = firstKey;
            this.map = new TreeMap<>(this);
        }

        TreePage setNextFirstKey(K nextFirstKey) {
            this.nextFirstKey = nextFirstKey;
            return this;
        }

        @Override
        public String toString() {
            return "tp[" + map.size() + "," + firstKey + "," + nextFirstKey + "]";
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
            return map.containsKey(key);
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getLastKey() {
            if (map.size() == 0) {
                return null;
            } else {
                return map.lastKey();
            }
        }

        @Override
        public V getValue(K key) {
            checkKey(key);
            PageValue pv = map.get(key);
            if (pv != null) {
                return pv.value();
            } else {
                return null;
            }
        }

        @Override
        public V getPutValue(K key, V val) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V getRemoveValue(K key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putValue(K key, V val) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeValue(K key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeValues(K start, K end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Map.Entry<K, V>> range(K start) {
            SortedMap<K, PageValue> tailMap = start != null ? map.tailMap(start) : map;
            if (log.isDebugEnabled()) {
                log.debug("range start=" + start + " tailMap=" + tailMap + " map=" + map);
            }
            return new TreePageIterator(tailMap);
        }

        @Override
        public K getNextFirstKey() {
            return nextFirstKey;
        }

        @Override
        public int compareKeys(K k1, K k2) {
            return ReadExternalPagedStore.this.compareKeys(k1, k2);
        }

        @Override
        public int compare(K o1, K o2) {
            return compareKeys(o1, o2);
        }
    }

    /**
     * allows iterators to preserve deferred decoding
     * <p/>
     * iterates over K,V pairs (non-page Ks to non-paged Vs)
     * <p/>
     * used by TreePage to provide an iterator
     * <p/>
     * TODO delete class
     */
    private final class TreePageIterator implements Iterator<Map.Entry<K, V>> {

        private final Iterator<Map.Entry<K, PageValue>> iter;

        private final class TreePageIterEntry implements Map.Entry<K, V> {

            private final Map.Entry<K, PageValue> entry;

            TreePageIterEntry(Map.Entry<K, PageValue> entry) {
                this.entry = entry;
            }

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
                throw new UnsupportedOperationException();
            }
        }

        //Is handed a portion of a TreePage's TreeMap (as a sorted map named tailmap).
        public TreePageIterator(SortedMap<K, PageValue> tailMap) {
            iter = tailMap.entrySet().iterator();
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
        public Map.Entry<K, V> next() {
            return new TreePageIterEntry(iter.next());
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    /**
     * since we have the nextFirstKey pointer in the TreePage class, we use our own iterator instead of one
     * from the backing page store.
     * <p/>
     * this would be less efficient if there is a significant cost to using store.get(key) over the pages
     * returned by the store's iterator. I do not believe that to be the case.
     */
    public Iterator<KeyValuePage<K, V>> getPageIterator(final K start) {
        return new PageIterator(start);
    }

    /**
     * iterates over K-(pages of V) entry objects.
     * essentially this iterates over k-page pairs.
     * <p/>
     * Handles decoding and interacting with the page cache.
     * <p/>
     * TODO: optionally(?) prebuffer the next page or delegate that to a sub-iterator
     * TODO: keep pointer to page and next key instead of two pages
     */
    private final class PageIterator implements Iterator<KeyValuePage<K, V>> {

        private KeyValuePage<K, V> nextPage;
        private KeyValuePage<K, V> page;

        public PageIterator(K start) {
            if (start == null) {
                start = getFirstKey();
            }
            nextPage = getOrLoadPageForKey(start);
        }

        @Override
        public String toString() {
            return "PI:" + page;
        }

        @Override
        public boolean hasNext() {
            fillNext();
            return (nextPage != null);
        }

        private void fillNext() {
            if (nextPage == null && page != null) {
                K nextPageKey = page.getNextFirstKey();
                if (nextPageKey != null) {
                    nextPage = getOrLoadPageForKey(nextPageKey);
                }
            }
        }

        @Override
        public KeyValuePage<K, V> next() {
            if (hasNext()) {
                page = nextPage;
                nextPage = null;
                return page;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public Iterator<Map.Entry<K, V>> range(K start) {
        //create PageIterator (to get a stream of pages), then create a bounded iterator
        return new BoundedIterator(getPageIterator(start), start);
    }


    /**
     * legacy comment: wrapper for page iterator
     * <p/>
     * yet another iterator wrapper. This one iterates over k-v pairs (not k-page pairs).
     * in this sense, it is similar to TreePageIterator (not to be confused with PageIterator)
     * <p/>
     * By bounded, it means bounded on the LEFT side only! It will iterate from START to the end
     * of the database; going by k-v pairs.
     * <p/>
     * TODO ----
     * Probably should try to buffer next page in many cases.
     */
    private class BoundedIterator implements ClosableIterator<Map.Entry<K, V>> {

        private K firstKey;
        //backing PageIterator (provides pages)
        private Iterator<KeyValuePage<K, V>> pageIterator;
        //backing ValueIterator (iterates over a page)
        private Iterator<Map.Entry<K, V>> valueIterator;
        //TreePage
        private KeyValuePage<K, V> nextPage;
        //K-V pairs
        private Map.Entry<K, V> lastEntry;
        private Map.Entry<K, V> nextEntry;

        BoundedIterator(Iterator<KeyValuePage<K, V>> iterator, K firstKey) {
            this.pageIterator = iterator;
            this.firstKey = firstKey;
        }

        @Override
        public String toString() {
            return "BI:" + firstKey + "," + pageIterator + "," + valueIterator + "," + nextPage + "," + lastEntry + "," + nextEntry;
        }

        private void fillNext() {
            /* first make sure we have a viable page */
            while (valueIterator == null && pageIterator != null && pageIterator.hasNext()) {
                nextPage = pageIterator.next();
                valueIterator = nextPage.range(firstKey);
                if (!valueIterator.hasNext()) {
                    valueIterator = null;
                }
            }
            /* make sure we have a viable page iterator */
            if (nextEntry == null && valueIterator != null && valueIterator.hasNext()) {
                nextEntry = valueIterator.next();
                if (!valueIterator.hasNext()) {
                    valueIterator = null;
                    nextPage = null;
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            fillNext();
            return nextEntry != null;
        }

        @Override
        public Map.Entry<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastEntry = nextEntry;
            nextEntry = null;
            return lastEntry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public int compareKeys(K k1, K k2) {
        return k1.compareTo(k2);
    }

    public ExternalPagedStoreMetrics getMetrics() {
        return metrics;
    }

    public void testIntegrity() {
        int counter = 0;
        int failedPages = 0;
        try {
            byte[] encodedKey = pages.firstKey();
            K key = keyCoder.keyDecode(encodedKey);
            do {
                KeyValuePage<K, V> newPage = loadingPageCache.get(key);
                byte[] encodedNextKey = pages.higherKey(encodedKey);
                if (encodedNextKey != null) {
                    K nextKey = keyCoder.keyDecode(encodedNextKey);
                    K nextFirstKey = newPage.getNextFirstKey();
                    K firstKey = newPage.getFirstKey();
                    K lastKey = newPage.getLastKey();
                    if (nextFirstKey == null) {
                        failedPages++;
                        log.warn("On page " + counter + " the firstKey is " +
                                 firstKey +
                             " the nextFirstKey is null" +
                             " and the next page is associated with key " + nextKey);
                        assert(false);
                    } else if (!nextFirstKey.equals(nextKey)) {
                        failedPages++;
                        int compareTo = compareKeys(nextFirstKey, nextKey);
                        char direction = compareTo > 0 ? '>' : '<';
                        log.warn("On page " + counter + " the firstKey is " +
                                 firstKey +
                             " the nextFirstKey is " + nextFirstKey +
                             " which is " + direction + " the next page is associated with key " + nextKey);
                        assert(false);
                    } else if (lastKey != null && compareKeys(lastKey,nextKey) >= 0) {
                        failedPages++;
                        log.warn("On page " + counter + " the firstKey is " +
                                 firstKey + " the largest key is " + lastKey +
                                 " the next key is " + nextKey +
                                 " which is less than or equal to the largest key.");
                        assert(false);
                    }
                    key = nextKey;
                }
                encodedKey = encodedNextKey;
                counter++;
                if (counter % 10000 == 0) {
                    log.info("Scanned " + counter + " pages. Detected " + failedPages + " failed pages.");
                }
            } while (encodedKey != null);
        } catch (ExecutionException ex) {
            log.error(ex.toString());
        }
        log.info("Scan complete. Scanned " + counter + " pages. Detected " + failedPages + " failed pages.");
    }

}

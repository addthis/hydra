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
package com.addthis.hydra.store.nonconcurrent;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.common.AbstractPageCache;
import com.addthis.hydra.store.common.ExternalMode;
import com.addthis.hydra.store.common.Page;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.util.MetricsUtil;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The {@link NonConcurrentPageCache} provides a paging data cache but does not offer
 * any concurrency protection.  Clients that use this cache should either be single threaded
 * or implement their own locking mechanisms.
 *
 * Evictions required to page new data into the cache happen synchronously with the operation
 * that requested new data from the backing store.
 *
 * @param <K> the key used to get/put values onto pages maintained by the cache
 * @param <V> the value which must extend {@link BytesCodable}
 */
public class NonConcurrentPageCache<K, V extends BytesCodable> extends AbstractPageCache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(NonConcurrentPageCache.class);


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
        protected int maxPages = defaultMaxPages;
        protected PageFactory<K, V> pageFactory = NonConcurrentPage.NonConcurrentPageFactory.singleton;

        public Builder(KeyCoder<K, V> keyCoder, ByteStore store, int maxPageSize) {
            this.externalStore = store;
            this.maxPageSize = maxPageSize;
            this.keyCoder = keyCoder;
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

        public NonConcurrentPageCache<K, V> build() {
            return new NonConcurrentPageCache<>(keyCoder, externalStore, maxPageSize,
                    maxPages, pageFactory);
        }

    }

    public NonConcurrentPageCache(KeyCoder<K, V> keyCoder, ByteStore externalStore, int maxPageSize,
                                  int maxPages, PageFactory<K, V> pageFactory) {
        super(keyCoder, externalStore, pageFactory, maxPageSize, maxPages, false);

        log.info("[init] ro=" + isReadOnly() + " maxPageSize=" + maxPageSize +
                " maxPages=" + maxPages + " gztype=" + NonConcurrentPage.gztype + " gzlevel=" +
                NonConcurrentPage.gzlevel + " gzbuf=" + NonConcurrentPage.gzbuf + " mem[page=" + mem_page + " type=NonConcurrentPageCache]");

    }


    public V remove(K key) {
        return doRemove(key);
    }

    protected V doPut(K key, V value) {
        V prev;

        evictAsneeded();

        Page<K, V> page = locatePage(key);

        prev = putIntoPage(page, key, value);

        int prevMem = page.getMemoryEstimate();
        page.updateMemoryEstimate();
        updateMemoryEstimate(page.getMemoryEstimate() - prevMem);

        if (page.splitCondition()) {
            splitPage(page);
        } else if (page.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
            page.setState(ExternalMode.DISK_MEMORY_DIRTY);
        }

        return prev;
    }

    /**
     * evict pages until we have room for additional
     * pages to enter the cache
     */
    private void evictAsneeded() {
        while (mustEvictPage()) {
            fixedNumberEviction(fixedNumberEvictions);
        }
    }

    @Override
    protected void doRemove(K start, K end) {
        while (true) {
            evictAsneeded();

            Page<K, V> page = locatePage(start);
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
                deletePage(targetKey);
                continue;
            } else if (endOffset == pageSize) {
                byte[] higherKeyEncoded = externalStore.higherKey(keyCoder.keyEncode(page.getFirstKey()));
                if (higherKeyEncoded != null) {
                    start = keyCoder.keyDecode(higherKeyEncoded);
                    continue;
                }
            }
            break;
        }
    }

    protected V doRemove(K key) {
        // even though we are removing data from the cache we may
        // need to page new data into the cache to perform that removal
        evictAsneeded();

        Page<K, V> page = locatePage(key);
        if (page.size() == 0) {
            if (!page.getFirstKey().equals(negInf)) {
                K targetKey = page.getFirstKey();
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
                deletePage(targetKey);
            }

            return prev;
        } else {
            return null;
        }

    }

    /**
     * Close without scheduling any unfinished background tasks.
     * The background eviction thread(s) are shut down regardless of
     * whether the skiplist exceeds its heap capacity.
     */
    @Override
    public void close() {
        doClose(false, CloseOperation.NONE);
    }

    /**
     * Close the cache.
     *
     * @param cleanLog  if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     * @return status code. A status code of 0 indicates success.
     */
    @Override
    public int close(boolean cleanLog, CloseOperation operation) {
        return doClose(cleanLog, operation);
    }


    @VisibleForTesting
    protected int doClose(boolean cleanLog, CloseOperation operation) {
        int status = 0;
        if (!shutdownGuard.getAndSet(true)) {

            pushAllPagesToDisk();
            if (operation != null && operation.testIntegrity()) {
                int failedPages = testIntegrity(operation.repairIntegrity());
                status = (failedPages > 0) ? 1 : 0;
            }
            closeExternalStore(cleanLog);
            assert (status == 0);
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
     * Return true if the page was evicted from the cache
     *
     * @return true if the page was evicted from the cache
     */
    protected boolean removePageFromCache(K targetKey) {
        assert (!targetKey.equals(negInf));

        Page<K, V> currentPage;
        Map.Entry<K, Page<K, V>> prevEntry, currentEntry;
        prevEntry = getCache().lowerEntry(targetKey);

        currentEntry = getCache().higherEntry(prevEntry.getKey());
        if (currentEntry != null) {
            currentPage = currentEntry.getValue();
            int compareKeys = compareKeys(targetKey, currentPage.getFirstKey());
            if (compareKeys < 0) {
                return false;
            } else if (compareKeys == 0 && currentPage.keys() == null &&
                    currentPage.getState() == ExternalMode.DISK_MEMORY_IDENTICAL) {
                currentPage.setState(ExternalMode.MEMORY_EVICTED);
                getCache().remove(targetKey);
                cacheSize.getAndDecrement();
                return true;
            }
        }
        return false;
    }

    protected void addToPurgeSet(Page<K, V> page) {
        if (!page.getFirstKey().equals(negInf)) {
            K minKey = page.getFirstKey();
            removePageFromCache(minKey);
        }
    }


}

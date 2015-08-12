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

/**
 * ::TWO INVARIANTS TO AVOID DEADLOCK AND MAINTAIN CONSISTENCY::
 * <p>
 * Invariant #1:
 * When locking two pages always lock the lower page before locking the higher page.
 * <p>
 * Invariant #2:
 * To read a consistent snapshot of a page in the external storage you must
 * be holding a lock on the lower page in memory.
 * <p>
 * The left sentinel page is the lowest page in storage. It is constructed with
 * a special first key with value negative infinity. No key may be smaller than
 * negative infinity. The left sentinel page may be neither <i>purged</i>
 * nor <i>deleted</i> (see below).
 * <p>
 * A page is <i>evicted</i> when the contents of the page are transferred from
 * the JVM heap into the external storage. When a page is evicted a page stub
 * remains in memory that contains the minimal information needed to restore the
 * page into memory.
 * <p>
 * A page is <i>purged</i> when a page stub is deleted from memory. The most
 * recent copy of this page still resides in the external storage. The left
 * sentinel page may not be purged.
 * <p>
 * A page is <i>deleted</i> when it is removed from both memory and the external storage.
 * Only pages with 0 keys may be deleted. The left sentinel page may not be deleted.
 *
 * @param <K>
 * @param <V>
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
                NonConcurrentPage.gzlevel + " gzbuf=" + NonConcurrentPage.gzbuf + " mem[page=" + mem_page + "]");

    }


    public V remove(K key) {
        return doRemove(key);
    }

    protected V doPut(K key, V value) {
        V prev;

        /**
         * If the background eviction threads are behind schedule,
         * then synchronously perform a page eviction. The
         * {@link #getEvictionTask()} and {@link #putEvictionTask(BackgroundEvictionTask)}
         * method are for re-using BackgroundEvictionTask object.
         */
        while (shouldEvictPage() || mustEvictPage()) {
            fixedNumberEviction(fixedNumberEvictions);
        }

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

    @Override
    protected void doRemove(K start, K end) {
        while (true) {
            while (shouldEvictPage() || mustEvictPage()) {
                fixedNumberEviction(fixedNumberEvictions);
            }

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
        while (shouldEvictPage() || mustEvictPage()) {
            fixedNumberEviction(fixedNumberEvictions);
        }

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

    protected void addToPurgeSet(Page<K, V> page) {
        if (!page.getFirstKey().equals(negInf)) {
            K minKey = page.getFirstKey();
            removePageFromCache(minKey);
        }
    }


}

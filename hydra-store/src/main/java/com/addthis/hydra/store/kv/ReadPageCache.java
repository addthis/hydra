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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.IReadPageDB;
import com.addthis.hydra.store.db.PageKey;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ReadPageCache<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
    implements IReadPageDB<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ReadPageCache.class);

    /**
     * guava loading cache for storing pages. Get method takes the exact page key, so finding the
     * page key must be done first.
     */
    final LoadingCache<K, ReadTreePage<K, V>> loadingPageCache;

    //backing byte store
    final ByteStore pages;

    final KeyCoder<K, V> keyCoder;

    public ReadPageCache(final KeyCoder<K, V> keyCoder, final ByteStore pages, int maxSize,
            int maxWeight) {
        this.keyCoder = keyCoder;
        this.pages = pages;
        log.info("[init] maxSize={} maxWeight={}", maxSize, maxWeight);

        // Prefer evicting on weight instead of page count
        if (maxWeight != 0) {
            loadingPageCache = CacheBuilder.newBuilder()
                    .weigher(ReadWeigher.INSTANCE)
                    .maximumWeight(maxWeight)
                    .build(new ReadPageCacheLoader<>(pages, keyCoder));
        } else {
            loadingPageCache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .build(new ReadPageCacheLoader<>(pages, keyCoder));
        }
    }

    public KeyCoder<K, V> getKeyCoder() {
        return keyCoder;
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
        try {
            return loadingPageCache.get(pageKey);
        } catch (ExecutionException | NullPointerException ignored) {
            return null;
        }
    }

    /**
     * TODO: Might as well store TreePage keys as undecoded bytes if we only use this method?
     */
    @Override
    public V get(K key) {
        KeyValuePage<K, V> page = getOrLoadPageForKey(key);
        return page.getValue(key);
    }

    @Override
    public void close() {
        pages.close();
    }

    /**
     * since we have the nextFirstKey pointer in the TreePage class, we use our own iterator instead of one
     * from the backing page store.
     * <p/>
     * this would be less efficient if there is a significant cost to using store.get(key) over the pages
     * returned by the store's iterator. I do not believe that to be the case.
     */
    public Iterator<KeyValuePage<K, V>> getPageIterator(final K start) {
        return new ReadPageIterator<>(this, start);
    }

    public Iterator<KeyValuePage<K, V>> getPageIterator(final K start,
            final boolean inclusive,
            final int sampleRate) {
        if (sampleRate < 2) {
            return new ReadPageIterator<>(this, start);
        } else {
            return new SampledPageIterator<>(this, start, inclusive, sampleRate);
        }
    }

    public Iterator<Map.Entry<K, V>> range(K start, boolean inclusive, int sampleRate) {
        //create PageIterator (to get a stream of pages), then create a bounded iterator
        return new LeftBoundedPagesToPairsIterator<>(getPageIterator(start, inclusive, sampleRate), start, inclusive);
    }

    //Uses the DR object from below.
    @Override
    public Iterator<Map.Entry<K, V>> range(K from, K to) {
        return new BoundedPagesToPairsIterator<>(this, from, to, 1);
    }

    public Iterator<Map.Entry<K, V>> range(K from, K to, int sampleRate) {
        return new BoundedPagesToPairsIterator<>(this, from, to, sampleRate);
    }
}

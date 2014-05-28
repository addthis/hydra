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
import java.util.SortedMap;
import java.util.TreeMap;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a page. Is constructed by pageDecode. See var comments
 */
class ReadTreePage<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        implements KeyValuePage<K, V>, IReadWeighable {

    private static final Logger log = LoggerFactory.getLogger(ReadTreePage.class);

    private final boolean checkKeyRange = Parameter.boolValue("eps.keys.debug", false);

    //An ordered mapping of K (individual/non-paged keys) to PageValues (decode-deferring wrappers for V)
    final TreeMap<K, PageValue<V>> map;

    //The first key in the map. also used as the key for this page in backing store
    final K firstKey;

    //The first of the next page (apparently). should also be the key for the next page in the backing store (?)
    //If this is always correct -> we can use our own key iterator without relying on one from backing store
    private K nextFirstKey;

    private int originalByteSize;

    ReadTreePage(K firstKey) {
        this.firstKey = firstKey;
        this.map = new TreeMap<>();
    }

    ReadTreePage<K, V> setNextFirstKey(K nextFirstKey) {
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
        if (map.isEmpty()) {
            return null;
        } else {
            return map.lastKey();
        }
    }

    @Override
    public V getValue(K key) {
        checkKey(key);
        PageValue<V> pv = map.get(key);
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
    public void removeValues(K start, K end, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<K, V>> range(K start, boolean inclusive) {
        SortedMap<K, PageValue<V>> tailMap;
        if (start != null) {
            tailMap = map.tailMap(start, inclusive);
        } else {
            tailMap = map;
        }
        log.debug("range start={} tailMap={} map={}", start, tailMap, map);
        return new PagesToPairsIterator<>(tailMap);
    }

    @Override
    public K getNextFirstKey() {
        return nextFirstKey;
    }

    @Override
    public int compareKeys(K k1, K k2) {
        return k1.compareTo(k2);
    }

    @Override
    public void setWeight(int weight) {
        originalByteSize = weight;
    }

    @Override
    public int getWeight() {
        return originalByteSize;
    }
}

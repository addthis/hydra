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
import java.util.Map.Entry;

import com.addthis.codec.codables.BytesCodable;

/**
 * highest level abstraction for a lexicographically sorted key/value store implementation
 *
 * @param <K>
 * @param <V>
 */
public interface KeyValueStore<K, V extends BytesCodable> {

    /**
     * @param key key for locating bound data
     * @return true if the store contains a value bound to the provided key
     */
    public boolean containsKey(K key);

    /**
     * key comparator for determining ordering
     *
     * @param k1
     * @param k2
     * @return less than zero if k1 precedes k2, zero if they're equals, otherwise greater than zero
     */
    public int compareKeys(K k1, K k2);

    /**
     * @return lexicographical first bound key in store
     */
    public K getFirstKey();

    /**
     * @return lexicographical last bound key in store
     */
    public K getLastKey();

    /**
     * @param key key for locating bound value
     * @return value bound to provided key or null
     */
    public V getValue(K key);

    /**
     * @param key key to use for binding
     * @param val value to bind to provided key
     * @return previous bound value
     */
    public V getPutValue(K key, V val);

    /**
     * @param key key for locating bound value
     * @return previously bound value for provided key
     */
    public V getRemoveValue(K key);

    /**
     * @param key key to bind value
     * @param val value to bind against provided key
     */
    public void putValue(K key, V val);

    /**
     * removes any value bound to the provided key
     *
     * @param key key for locating bound value
     */
    public void removeValue(K key);

    /**
     * removes a range of (key, value) pairs
     *
     * @param start     lower bound of range deletion (inclusive)
     * @param end       upper bound of range deletion (exclusive)
     */
    public void removeValues(K start, K end);

    /**
     * lexicographically ordered iterator
     *
     * @param start     key or null for beginning
     * @return
     */
    public Iterator<Entry<K, V>> range(K start);
}

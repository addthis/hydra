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
package com.addthis.hydra.data.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class SparseArrayBackedMap<K, V> implements java.util.SortedMap<K, V> {

    private final int slots;

    public SparseArrayBackedMap(int size) {
        slots = nextClosestPowerOfTwo(size) - 1;
    }

    private int nextClosestPowerOfTwo(int val) {
        int pow = 0;
        while (val != 0) {
            pow++;
            val >>= 1;
        }
        return (int) Math.pow(2, pow);
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public V get(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V put(K key, V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V remove(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public Comparator<? super K> comparator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K firstKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K lastKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<K> keySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<V> values() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        // TODO Auto-generated method stub
        return null;
    }

}

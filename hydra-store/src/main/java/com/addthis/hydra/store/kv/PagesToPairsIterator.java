package com.addthis.hydra.store.kv;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

/**
 * allows iterators to preserve deferred decoding
 * <p/>
 * iterates over K,V pairs (non-page Ks to non-paged Vs)
 * <p/>
 * used by TreePage to provide an iterator
 * <p/>
 * TODO delete class
 */
class PagesToPairsIterator<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        implements Iterator<Map.Entry<K, V>> {

    private final Iterator<Map.Entry<K, PageValue<V>>> iter;

    private final class TreePageIterEntry implements Map.Entry<K, V> {

        private final Map.Entry<K, PageValue<V>> entry;

        TreePageIterEntry(Map.Entry<K, PageValue<V>> entry) {
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
    public PagesToPairsIterator(SortedMap<K, PageValue<V>> tailMap) {
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

package com.addthis.hydra.store.skiplist;

import java.util.ArrayList;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.KeyCoder.EncodeType;

public abstract class PageFactory<K,V extends Codec.BytesCodable> {

    abstract Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, EncodeType encodeType);

    abstract Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size,
            ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues,
            EncodeType encodeType);

    public final Page<K, V> generateEmptyPage(SkipListCache<K, V> cache,
            K firstKey, K nextFirstKey, EncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, encodeType);
    }

    public final Page<K, V> generateEmptyPage(SkipListCache<K, V> cache,
            K firstKey, EncodeType encodeType) {
        return newPage(cache, firstKey, null, encodeType);
    }

    public final Page<K, V> measureMemoryEmptyPage(EncodeType encodeType) {
        return newPage(null, null, null, encodeType);
    }

    public final Page<K,V> generateSiblingPage(SkipListCache<K, V> cache,
            K firstKey, K nextFirstKey,
            int size, ArrayList<K> keys,
            ArrayList<V> values,
            ArrayList<byte[]> rawValues, EncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }

}

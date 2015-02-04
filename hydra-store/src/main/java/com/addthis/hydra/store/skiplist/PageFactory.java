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
package com.addthis.hydra.store.skiplist;

import java.util.ArrayList;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.PageEncodeType;

public abstract class PageFactory<K,V extends BytesCodable> {

    abstract Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType);

    abstract Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size,
            ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues,
            PageEncodeType encodeType);

    public final Page<K, V> generateEmptyPage(SkipListCache<K, V> cache,
            K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, encodeType);
    }

    public final Page<K, V> generateEmptyPage(SkipListCache<K, V> cache,
            K firstKey, PageEncodeType encodeType) {
        return newPage(cache, firstKey, null, encodeType);
    }

    public final Page<K, V> measureMemoryEmptyPage(PageEncodeType encodeType) {
        return newPage(null, null, null, encodeType);
    }

    public final Page<K,V> generateSiblingPage(SkipListCache<K, V> cache,
            K firstKey, K nextFirstKey,
            int size, ArrayList<K> keys,
            ArrayList<V> values,
            ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }

}

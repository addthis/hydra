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
package com.addthis.hydra.store.common;

import java.util.ArrayList;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.kv.PagedKeyValueStore;

public abstract class PageFactory<K, V extends BytesCodable> {

    public enum TYPE {CONUCRRENT, NON_CONCURRENT}

    protected abstract Page<K, V> newPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType);

    protected abstract Page<K, V> newPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, int size,
                                          ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues,
                                          PageEncodeType encodeType);

    public abstract TYPE getType();

    public final Page<K, V> generateEmptyPage(AbstractPageCache<K, V> cache,
                                              K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, encodeType);
    }

    public final Page<K, V> generateEmptyPage(AbstractPageCache<K, V> cache,
                                              K firstKey, PageEncodeType encodeType) {
        return newPage(cache, firstKey, null, encodeType);
    }

    public final Page<K, V> measureMemoryEmptyPage(PageEncodeType encodeType) {
        return newPage(null, null, null, encodeType);
    }

    public final Page<K, V> generateSiblingPage(AbstractPageCache<K, V> cache,
                                                K firstKey, K nextFirstKey,
                                                int size, ArrayList<K> keys,
                                                ArrayList<V> values,
                                                ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
        return newPage(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }


}

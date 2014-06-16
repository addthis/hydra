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
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReadPageCaches {

    private static final Logger log = LoggerFactory.getLogger(ReadPageCaches.class);

    private ReadPageCaches() {
    }

    public static <K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
    void testIntegrity(ReadPageCache<K, V> readPageCache) {
        int counter = 0;
        int failedPages = 0;
        try {
            byte[] encodedKey = readPageCache.pages.firstKey();
            K key = readPageCache.keyCoder.keyDecode(encodedKey);
            do {
                KeyValuePage<K, V> newPage = readPageCache.loadingPageCache.get(key);
                byte[] encodedNextKey = readPageCache.pages.higherKey(encodedKey);
                if (encodedNextKey != null) {
                    K nextKey = readPageCache.keyCoder.keyDecode(encodedNextKey);
                    K nextFirstKey = newPage.getNextFirstKey();
                    K firstKey = newPage.getFirstKey();
                    K lastKey = newPage.getLastKey();
                    if (nextFirstKey == null) {
                        failedPages++;
                        log.warn("On page {} the firstKey is {} the nextFirstKey is null and the next page is associated with key {}", counter, firstKey, nextKey);
                        assert false;
                    } else if (!nextFirstKey.equals(nextKey)) {
                        failedPages++;
                        int compareTo = nextFirstKey.compareTo(nextKey);
                        char direction = compareTo > 0 ? '>' : '<';
                        log.warn("On page {} the firstKey is {} the nextFirstKey is {} which is {} the next page is associated with key {}", counter, firstKey, nextFirstKey, direction, nextKey);
                        assert false;
                    } else if (lastKey != null && lastKey.compareTo(nextKey) >= 0) {
                        failedPages++;
                        log.warn("On page {} the firstKey is {} the largest key is {} the next key is {} which is less than or equal to the largest key.", counter, firstKey, lastKey, nextKey);
                        assert false;
                    }
                    key = nextKey;
                }
                encodedKey = encodedNextKey;
                counter++;
                if (counter % 10000 == 0) {
                    log.info("Scanned {} pages. Detected {} failed pages.", counter, failedPages);
                }
            } while (encodedKey != null);
        } catch (ExecutionException ex) {
            log.error(ex.toString());
        }
        log.info("Scan complete. Scanned {} pages. Detected {} failed pages.", counter, failedPages);
    }

    public static <K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
    TreeMap<K, V> toTreeMap(ReadPageCache<K, V> readPageCache) {
        try {
            Iterator<Map.Entry<K, V>> range = readPageCache.range(readPageCache.getFirstKey(), readPageCache.keyCoder.posInfinity());
            TreeMap<K, V> map = new TreeMap<>();
            while (range.hasNext()) {
                Map.Entry<K, V> entry = range.next();
                map.put(entry.getKey(), entry.getValue());
            }
            return map;
        } catch (Exception e) {
            log.warn("failed to dump PageDB to Map, returning empty (expected for uninitialized db)", e);
            return new TreeMap<>();
        }
    }
}

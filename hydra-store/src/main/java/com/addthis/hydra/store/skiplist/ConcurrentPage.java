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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.common.AbstractPage;
import com.addthis.hydra.store.common.AbstractPageCache;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.kv.PageEncodeType;

/**
 * Concurrent implementation of {@link AbstractPage}.  Main purpose is to provide a
 * factory for generating instances of this class.
 *
 * @param <K> the key used to get/put values onto pages maintained by the cache
 * @param <V> the value which must extend {@link BytesCodable}
 */
public class ConcurrentPage<K, V extends BytesCodable> extends AbstractPage<K, V> {


    public ConcurrentPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, encodeType);
    }

    public ConcurrentPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }

    public ReentrantReadWriteLock initLock() {
        return new ReentrantReadWriteLock();
    }

    public static class ConcurrentPageFactory<K, V extends BytesCodable> extends PageFactory<K, V> {

        public static final ConcurrentPageFactory singleton = new ConcurrentPageFactory<>();

        @Override
        public TYPE getType() {
            return TYPE.CONUCRRENT;
        }

        private ConcurrentPageFactory() {
        }

        @Override
        protected ConcurrentPage<K, V> newPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
            return new ConcurrentPage<>(cache, firstKey, nextFirstKey, encodeType);
        }

        @Override
        protected ConcurrentPage<K, V> newPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
            return new ConcurrentPage<>(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
        }
    }
}


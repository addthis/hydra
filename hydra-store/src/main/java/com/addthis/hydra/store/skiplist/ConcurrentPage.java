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
import com.addthis.hydra.store.common.AbstractPage;
import com.addthis.hydra.store.common.AbstractPageCache;
import com.addthis.hydra.store.common.Page;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.kv.PageEncodeType;

public class ConcurrentPage<K, V extends BytesCodable> extends AbstractPage<K, V> {


    public ConcurrentPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, encodeType, Type.CONCURRENT);
    }

    public ConcurrentPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys, ArrayList<V> values, ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType, Type.CONCURRENT);
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


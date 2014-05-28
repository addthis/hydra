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
import java.util.NoSuchElementException;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

/**
 * iterates over K-(pages of V) entry objects.
 * essentially this iterates over k-page pairs.
 * <p/>
 * Handles decoding and interacting with the page cache.
 * <p/>
 * TODO: optionally(?) prebuffer the next page or delegate that to a sub-iterator
 * TODO: keep pointer to page and next key instead of two pages
 */
class ReadPageIterator<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        implements Iterator<KeyValuePage<K, V>> {

    private KeyValuePage<K, V> nextPage;
    private KeyValuePage<K, V> page;

    private final ReadPageCache<K, V> readPageCache;

    public ReadPageIterator(ReadPageCache<K, V> readPageCache, K start) {
        this.readPageCache = readPageCache;
        if (start == null) {
            start = readPageCache.getFirstKey();
        }
        nextPage = readPageCache.getOrLoadPageForKey(start);
    }

    @Override
    public String toString() {
        return "PI:" + page;
    }

    @Override
    public boolean hasNext() {
        fillNext();
        return nextPage != null;
    }

    private void fillNext() {
        if ((nextPage == null) && (page != null)) {
            K nextPageKey = page.getNextFirstKey();
            if (nextPageKey != null) {
                nextPage = readPageCache.getOrLoadPageForKey(nextPageKey);
            }
        }
    }

    @Override
    public KeyValuePage<K, V> next() {
        if (hasNext()) {
            page = nextPage;
            nextPage = null;
            return page;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

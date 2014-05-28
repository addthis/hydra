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
import java.util.NoSuchElementException;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.PageKey;

/**
 * legacy comment: wrapper for page iterator
 * <p/>
 * yet another iterator wrapper. This one iterates over k-v pairs (not k-page pairs).
 * in this sense, it is similar to TreePageIterator (not to be confused with PageIterator)
 * <p/>
 * By bounded, it means bounded on the LEFT side only! It will iterate from START to the end
 * of the database; going by k-v pairs.
 * <p/>
 * TODO ----
 * Probably should try to buffer next page in many cases.
 */
class LeftBoundedPagesToPairsIterator<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        implements Iterator<Map.Entry<K, V>> {

    private K firstKey;
    private boolean inclusive;
    //backing PageIterator (provides pages)
    private Iterator<KeyValuePage<K, V>> pageIterator;
    //backing ValueIterator (iterates over a page)
    private Iterator<Map.Entry<K, V>> valueIterator;
    //TreePage
    private KeyValuePage<K, V> nextPage;
    //K-V pairs
    private Map.Entry<K, V> lastEntry;
    private Map.Entry<K, V> nextEntry;

    LeftBoundedPagesToPairsIterator(Iterator<KeyValuePage<K, V>> iterator, K firstKey, boolean inclusive) {
        this.pageIterator = iterator;
        this.firstKey = firstKey;
        this.inclusive = inclusive;
    }

    @Override
    public String toString() {
        return "BI:" + firstKey + "," + inclusive + "," + pageIterator + "," + valueIterator + "," + nextPage + "," + lastEntry + "," + nextEntry;
    }

    private void fillNext() {
        /* first make sure we have a viable page */
        while (valueIterator == null && pageIterator != null && pageIterator.hasNext()) {
            nextPage = pageIterator.next();
            valueIterator = nextPage.range(firstKey, inclusive);
            if (!valueIterator.hasNext()) {
                valueIterator = null;
            }
        }
        /* make sure we have a viable page iterator */
        if (nextEntry == null && valueIterator != null && valueIterator.hasNext()) {
            nextEntry = valueIterator.next();
            if (!valueIterator.hasNext()) {
                valueIterator = null;
                nextPage = null;
            }
        }
    }

    @Override
    public boolean hasNext() {
        fillNext();
        return nextEntry != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        lastEntry = nextEntry;
        nextEntry = null;
        return lastEntry;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

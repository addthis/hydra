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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps eps range and adds RIGHT-side bounding (on top of the LEFT-side bounding
 * provided by the eps object). Not thread safe.
 */
class BoundedPagesToPairsIterator<K extends Comparable<K> & PageKey, V extends IReadWeighable & Codec.BytesCodable>
        implements Iterator<Map.Entry<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(BoundedPagesToPairsIterator.class);

    private final Iterator<Map.Entry<K, V>> iter;
    private final K to;

    private Map.Entry<K, V> next;

    BoundedPagesToPairsIterator(ReadPageCache<K, V> readPageCache, K start, K to, int sampleRate) {
        log.debug("DR({}-{})", start, to);
        this.iter = readPageCache.range(start, true, sampleRate);
        this.to = to;
    }

    @Override
    public boolean hasNext() {
        if ((next == null) && iter.hasNext()) {
            next = iter.next();
            if ((to != null) && (next.getKey().compareTo(to) >= 0)) {
                log.debug("stopping range b/c {} >= {}", next.getKey(), to);
                next = null;
            }
        }
        return next != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            Map.Entry<K, V> ret = next;
            next = null;
            return ret;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

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
package com.addthis.hydra.data.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.addthis.codec.Codec;


/**
 * maintains a list of the number of keys in each
 * value bucket.  buckets are configurable as
 * powers of N (from the scale setting).
 * <p/>
 * TODO store # of things in each bucket as well as total of key values in each bucket
 */
public class KeyHistogram implements Codec.SuperCodable {

    @Codec.Set(codable = true)
    private HashMap<Long, Long> map;
    @Codec.Set(codable = true)
    private int scale = 10;

    public KeyHistogram init() {
        map = new HashMap<Long, Long>();
        return this;
    }

    public KeyHistogram setScale(int scale) {
        if (scale < 2) {
            throw new RuntimeException("scale values must be >= 2");
        }
        this.scale = scale;
        return this;
    }

    public Map<Long, Long> getHistogram() {
        return map;
    }

    public Map<Long, Long> getSortedHistogram() {
        TreeMap<Long, Long> sort = new TreeMap<Long, Long>();
        for (Entry<Long, Long> e : map.entrySet()) {
            sort.put(e.getKey(), e.getValue());
        }
        return sort;
    }

    public boolean incrementFrom(long from) {
        return update(from, from + 1);
    }

    public boolean incrementTo(long to) {
        return update(to - 1, to);
    }

    /**
     * @return returns true if the map was changed by the update
     */
    public boolean update(long from, long to) {
        if (from == to) {
            return false;
        }
        if (from == 0) {
            incTo(getBucket(to));
            return true;
        }
        if (to == 0) {
            decFrom(getBucket(from));
            return true;
        }
        long bucketFrom = getBucket(from);
        long bucketTo = getBucket(to);
        if (bucketFrom != bucketTo) {
            incTo(bucketTo);
            decFrom(bucketFrom);
            return true;
        }
        return false;
    }

    private void decFrom(long bucketFrom) {
        Long val = Long.valueOf(bucketFrom);
        Long dec = map.get(val);
        if (dec == null || dec == 0) {
            map.put(val, 0L);
        } else {
            map.put(val, dec - 1);
        }
    }

    private void incTo(long bucketTo) {
        Long val = Long.valueOf(bucketTo);
        Long inc = map.get(val);
        if (inc == null) {
            map.put(val, 1L);
        } else {
            map.put(val, inc + 1);
        }
    }

    private long getBucket(long val) {
        long compare = 1;
        long last = compare;
        while (val >= compare) {
            last = compare;
            compare *= scale;
        }
        return last;
    }

    @Override
    public void postDecode() {
        if (map == null) {
            init();
        }
    }

    @Override
    public void preEncode() {
    }
}

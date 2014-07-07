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
package com.yammer.metrics.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.codec.annotations.FieldConfig;

/**
 * A codable version of Sample from CodaHales metrics project
 * <p/>
 * https://github.com/codahale/metrics/blob/master/metrics-core/src/main/java/com/yammer/metrics/stats/Sample.java
 */
public class CodableUniformSample implements Sample {

    private static final int BITS_PER_LONG = 63;

    @FieldConfig(codable = true)
    private AtomicLong count = new AtomicLong();
    @FieldConfig(codable = true)
    private int reservoirSize;
    @FieldConfig(codable = true)
    private AtomicLong[] values;

    public CodableUniformSample init(int reservoirSize) {
        this.reservoirSize = reservoirSize;
        values = new AtomicLong[reservoirSize];
        clear();
        return this;
    }

    @Override
    public int size() {
        final long c = count.get();
        if (c > values.length) {
            return values.length;
        }
        return (int) c;
    }

    @Override
    public void clear() {
        for (int i = 0; i < values.length; i++) {
            values[i] = new AtomicLong(0);
        }
        count.set(0);
    }

    @Override
    public void update(long value) {
        final long c = count.incrementAndGet();
        if (c <= values.length) {
            values[(int) c - 1].set(value);
        } else {
            final long r = nextLong(c);
            if (r < values.length) {
                values[(int) r].set(value);
            }
        }
    }

    /**
     * Get a pseudo-random long uniformly between 0 and n-1. Stolen from
     * {@link java.util.Random#nextInt()}.
     *
     * @param n the bound
     * @return a value select randomly from the range {@code [0..n)}.
     */
    private static long nextLong(long n) {
        long bits, val;
        do {
            bits = ThreadLocalRandom.current().nextLong() & (~(1L << BITS_PER_LONG));
            val = bits % n;
        } while (bits - val + (n - 1) < 0L);
        return val;
    }

    @Override
    public Snapshot getSnapshot() {
        final int s = size();
        final List<Long> copy = new ArrayList<Long>(s);
        for (int i = 0; i < s; i++) {
            copy.add(values[i].get());
        }
        return new Snapshot(copy);
    }
}

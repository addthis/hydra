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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.addthis.codec.Codec;

import com.yammer.metrics.stats.CodableUniformSample;
import com.yammer.metrics.stats.Snapshot;

import static java.lang.Math.sqrt;

/**
 * Maintains a percentile distribution for a given key.
 * <p/>
 * This code heavily borrows heavily from CodaHale's Histogram in the metrics project.
 * <p/>
 * See https://github.com/codahale/metrics/blob/master/metrics-core/src/main/java/com/yammer/metrics/core/Histogram.java
 * <p/>
 * for the original source.  The main difference is that this version uses many codable
 * fields so that we can persist and re-hydrate the object using Codec.
 * <p/>
 * Note:  Currently this class only supports uniform sampling.  The value input should be a long
 */
public class KeyPercentileDistribution implements Codec.SuperCodable {

    @Codec.Set(codable = true)
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    @Codec.Set(codable = true)
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    @Codec.Set(codable = true)
    private final AtomicLong sum = new AtomicLong();

    // These are for the Welford algorithm for calculating running variance
    // without floating-point doom.
    private final AtomicReference<double[]> variance =
            new AtomicReference<>(new double[]{-1, 0}); // M, S
    @Codec.Set(codable = true)
    private final AtomicLong count = new AtomicLong();
    private final ArrayCache arrayCache = new ArrayCache();

    @Codec.Set(codable = true)
    private CodableUniformSample sample;
    @Codec.Set(codable = true)
    private double[] arrayCacheValue;
    @Codec.Set(codable = true)
    private double[] varianceValues;
    @Codec.Set(codable = true)
    private int sampleSize;

    public KeyPercentileDistribution setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return this;
    }

    public KeyPercentileDistribution init() {
        sample = new CodableUniformSample().init(sampleSize);
        return this;
    }

    /**
     * Cache arrays for the variance calculation, so as to avoid memory allocation.
     */
    private static class ArrayCache extends ThreadLocal<double[]> {

        @Override
        protected double[] initialValue() {
            return new double[2];
        }
    }

    @Override
    public void postDecode() {

        if (sample == null) {
            sample = new CodableUniformSample().init(sampleSize);
        }
        if (arrayCacheValue != null) {
            arrayCache.set(arrayCacheValue);
        }
        if (varianceValues != null) {
            variance.set(varianceValues);
        }
    }

    @Override
    public void preEncode() {
        arrayCacheValue = arrayCache.get();
        varianceValues = variance.get();
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(int value) {
        update((long) value);
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(long value) {
        count.incrementAndGet();
        sample.update(value);
        setMax(value);
        setMin(value);
        sum.getAndAdd(value);
        updateVariance(value);
    }

    /**
     * Returns the number of values recorded.
     *
     * @return the number of values recorded
     */
    public long count() {
        return count.get();
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#max()
    */
    public long max() {
        if (count() > 0) {
            return max.get();
        }
        return 0;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#min()
    */
    public long min() {
        if (count() > 0) {
            return min.get();
        }
        return 0;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#mean()
    */
    public double mean() {
        if (count() > 0) {
            return sum.get() / count();
        }
        return 0;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#stdDev()
    */
    public double stdDev() {
        if (count() > 0) {
            return sqrt(variance());
        }
        return 0.0;
    }

    public Snapshot getSnapshot() {
        return sample.getSnapshot();
    }

    private double variance() {
        if (count() <= 1) {
            return 0.0;
        }
        return variance.get()[1] / (count() - 1);
    }

    private void setMax(long potentialMax) {
        while (true) {
            long currentMax = max.get();
            if (potentialMax <= currentMax) {
                return;
            }
            if (max.compareAndSet(currentMax, potentialMax)) {
                return;
            }
        }
    }

    private void setMin(long potentialMin) {
        while (true) {
            long currentMin = min.get();
            if (potentialMin >= currentMin) {
                return;
            }
            if (min.compareAndSet(currentMin, potentialMin)) {
                return;
            }
        }
    }

    private void updateVariance(long value) {
        boolean done = false;
        while (!done) {
            final double[] oldValues = variance.get();
            final double[] newValues = arrayCache.get();
            if (oldValues[0] == -1) {
                newValues[0] = value;
                newValues[1] = 0;
            } else {
                final double oldM = oldValues[0];
                final double oldS = oldValues[1];

                final double newM = oldM + ((value - oldM) / count());
                final double newS = oldS + ((value - oldM) * (value - newM));

                newValues[0] = newM;
                newValues[1] = newS;
            }
            done = variance.compareAndSet(oldValues, newValues);
            if (done) {
                // recycle the old array into the cache
                arrayCache.set(oldValues);
            }
        }
    }


}

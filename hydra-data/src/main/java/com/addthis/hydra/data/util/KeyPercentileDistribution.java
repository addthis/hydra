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

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;

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
public class KeyPercentileDistribution implements SuperCodable {

    @FieldConfig(codable = true)
    private long min = Long.MAX_VALUE;
    @FieldConfig(codable = true)
    private long max = Long.MIN_VALUE;
    @FieldConfig(codable = true)
    private long sum = 0;
    @FieldConfig(codable = true)
    private long count = 0;

    @FieldConfig(codable = true)
    private CodableUniformSample sample;
    @FieldConfig(codable = true)
    private double[] arrayCacheValue;
    @FieldConfig(codable = true)
    private double[] varianceValues;
    @FieldConfig(codable = true)
    private int sampleSize;

    // These are for the Welford algorithm for calculating running variance
    // without floating-point doom.
    private transient double m;
    private transient double s;

    // for CodecBin2
    public KeyPercentileDistribution() {}

    public KeyPercentileDistribution(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public KeyPercentileDistribution init() {
        sample = new CodableUniformSample().init(sampleSize);
        return this;
    }

    @Override
    public void postDecode() {
        if (sample == null) {
            sample = new CodableUniformSample().init(sampleSize);
        }
    }

    @Override public void preEncode() {}

    /** Adds a recorded value. */
    public void update(int value) {
        update((long) value);
    }

    /** Adds a recorded value. */
    public void update(long value) {
        count += 1;
        sample.update(value);
        setMax(value);
        setMin(value);
        sum += value;
        updateVariance(value);
    }

    /** Returns the number of values recorded. */
    public long count() {
        return count;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#max()
    */
    public long max() {
        if (count() > 0) {
            return max;
        }
        return 0;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#min()
    */
    public long min() {
        if (count() > 0) {
            return min;
        }
        return 0;
    }

    /* (non-Javadoc)
    * @see com.yammer.metrics.core.Summarizable#mean()
    */
    public double mean() {
        if (count() > 0) {
            return (double) (sum / count());
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
        return s / (double) (count() - 1);
    }

    private void setMax(long potentialMax) {
        max = Math.max(max, potentialMax);
    }

    private void setMin(long potentialMin) {
        min = Math.min(min, potentialMin);
    }

    private void updateVariance(long value) {
        final double oldM = m;
        final double oldS = s;
        if (oldM == -1) {
            m = value;
            s = 0;
        } else {
            final double newM = oldM + ((value - oldM) / count());
            final double newS = oldS + ((value - oldM) * (value - newM));

            m = newM;
            s = newS;
        }
    }
}

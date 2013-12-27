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
package com.addthis.hydra.util;

import com.clearspring.analytics.stream.quantile.IQuantileEstimator;
import com.clearspring.analytics.stream.quantile.QDigest;

public class Histogram {

    private final IQuantileEstimator quantiles;
    private long count;
    private double mean;
    private double m2;
    private long total;
    private long min;
    private long max;

    public Histogram() {
        this.quantiles = new QDigest(1000);
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    public void offer(long value) {
        count++;
        total += value;
        quantiles.offer(value);
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
    }

    public long getTotal() {
        return total;
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        return m2 / (count - 1);
    }

    public long getCount() {
        return count;
    }

    public long getQuantile(double q) {
        return quantiles.getQuantile(q);
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

}

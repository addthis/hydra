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
package com.addthis.hydra.store.kv.metrics;

import javax.annotation.Nonnull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

public class ExternalPagedStoreMetrics {

    private static final AtomicInteger scopeGenerator = new AtomicInteger();

    @Nonnull
    private final AtomicLong getHits;

    @Nonnull
    private final AtomicLong getMisses;

    @Nonnull
    private final String metricsScope;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Long> getHitsGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Long> getMissesGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Double> missesRatioGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Histogram pageSize;

    public ExternalPagedStoreMetrics() {
        getHits = new AtomicLong();
        getMisses = new AtomicLong();

        metricsScope = "ExternalPagedStoreMetrics" + Integer.toString(scopeGenerator.getAndIncrement());

        getHitsGauge = Metrics.newGauge(getClass(), "getHits", metricsScope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return getHits.get();
                    }
                });

        getMissesGauge = Metrics.newGauge(getClass(), "getMisses", metricsScope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return getMisses.get();
                    }
                });

        missesRatioGauge = Metrics.newGauge(getClass(), "getMissRatio", metricsScope,
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        long hits = getHits.get();
                        long misses = getMisses.get();
                        return (misses / ((double) (hits + misses)));
                    }
                });

        pageSize = Metrics.newHistogram(getClass(), "pageSize", metricsScope);
    }

    public void updatePageSize(int value) {
        pageSize.update(value);
    }


    public void updateGetValue(Object value) {
        if (value == null) {
            getMisses.getAndIncrement();
        } else {
            getHits.getAndIncrement();
        }
    }

    public Histogram getPageSize() {
        return pageSize;
    }
}

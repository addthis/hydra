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
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class CachedPagedStoreMetrics {

    private static final AtomicInteger scopeGenerator = new AtomicInteger();

    @Nonnull
    private final AtomicLong pageHits;

    @Nonnull
    private final AtomicLong pageMisses;

    @Nonnull
    private final String metricsScope;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Long> pageHitsGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Long> pageMissesGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Double> missesRatioGauge;

    @Nonnull
    @SuppressWarnings("unused")
    private final Timer cacheMissesTimer;

    @Nonnull
    @SuppressWarnings("unused")
    private final Gauge<Double> missesRatioTimerSum;


    public CachedPagedStoreMetrics() {
        pageHits = new AtomicLong();
        pageMisses = new AtomicLong();

        metricsScope = "CachedPagedStoreMetrics" + Integer.toString(scopeGenerator.getAndIncrement());

        pageHitsGauge = Metrics.newGauge(getClass(), "pageHits", metricsScope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return pageHits.get();
                    }
                });

        pageMissesGauge = Metrics.newGauge(getClass(), "pageMisses", metricsScope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return pageMisses.get();
                    }
                });

        missesRatioGauge = Metrics.newGauge(getClass(), "pageMissRatio", metricsScope,
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        long hits = pageHits.get();
                        long misses = pageMisses.get();
                        return (misses / ((double) (hits + misses)));
                    }
                });

        cacheMissesTimer = Metrics.newTimer(getClass(), "cacheMissTime", metricsScope);

        missesRatioTimerSum = Metrics.newGauge(getClass(), "cacheMissTotalTime", metricsScope,
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        return cacheMissesTimer.sum();
                    }
                });
    }


    public void recordPageCache(boolean cacheHit) {
        if (cacheHit) {
            pageHits.getAndIncrement();
        } else {
            pageMisses.getAndIncrement();
        }
    }

    public TimerContext startTimer() {
        return cacheMissesTimer.time();
    }
}

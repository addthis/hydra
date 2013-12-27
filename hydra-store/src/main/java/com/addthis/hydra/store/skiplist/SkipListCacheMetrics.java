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
package com.addthis.hydra.store.skiplist;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

public class SkipListCacheMetrics {

    private final SkipListCache parent;

    @SuppressWarnings("unused")
    final Gauge<Long> memEstimateGauge;

    @SuppressWarnings("unused")
    final Gauge<Integer> cacheSizeGauge;

    @SuppressWarnings("unused")
    final Gauge<Integer> pagesInMemoryGauge;

    @SuppressWarnings("unused")
    final Gauge<Long> pagesDeletedGauge;

    final Histogram encodeFirstKeySize;

    final Histogram encodeNextFirstKeySize;

    final Histogram encodeKeySize;

    final Histogram encodeValueSize;

    // This metrics counts compressed bytes
    final Histogram encodePageSize;

    // This metrics counts compressed bytes
    final Histogram numberKeysPerPage;

    public SkipListCacheMetrics(SkipListCache cache) {
        parent = cache;
        memEstimateGauge = Metrics.newGauge(SkipListCache.class,
                "memoryEstimate", parent.scope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return parent.getMemoryEstimate();
                    }
                });

        cacheSizeGauge = Metrics.newGauge(SkipListCache.class,
                "cacheSize", parent.scope,
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return parent.cacheSize.get();
                    }
                });

        pagesInMemoryGauge = Metrics.newGauge(SkipListCache.class,
                "pagesInMemory", parent.scope,
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return parent.numPagesInMemory.get();
                    }
                });

        pagesDeletedGauge = Metrics.newGauge(SkipListCache.class,
                "pagesDeleted", parent.scope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return parent.numPagesDeleted.get();
                    }
                });

        encodeFirstKeySize = SkipListCache.trackEncodingByteUsage ?
                             Metrics.newHistogram(SkipListCache.class, "encodeFirstKeySize", parent.scope) :
                             null;

        encodeNextFirstKeySize = SkipListCache.trackEncodingByteUsage ?
                                 Metrics.newHistogram(SkipListCache.class, "encodeNextFirstKeySize", parent.scope) :
                                 null;

        encodeKeySize = SkipListCache.trackEncodingByteUsage ?
                        Metrics.newHistogram(SkipListCache.class, "encodeKeySize", parent.scope) :
                        null;

        encodeValueSize = SkipListCache.trackEncodingByteUsage ?
                          Metrics.newHistogram(SkipListCache.class, "encodeValueSize", parent.scope) :
                          null;

        encodePageSize = SkipListCache.trackEncodingByteUsage ?
                         Metrics.newHistogram(SkipListCache.class, "encodePageSize", parent.scope) :
                         null;

        numberKeysPerPage = SkipListCache.trackEncodingByteUsage ?
                            Metrics.newHistogram(SkipListCache.class, "numberKeysPerPage", parent.scope) :
                            null;

    }

}

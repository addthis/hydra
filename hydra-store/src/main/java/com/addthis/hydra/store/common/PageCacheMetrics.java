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
package com.addthis.hydra.store.common;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPageCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

public class PageCacheMetrics<K, V extends BytesCodable> {

    public final AbstractPageCache<K, V> parent;

    @SuppressWarnings("unused")
    public final Gauge<Long> memEstimateGauge;

    @SuppressWarnings("unused")
    public final Gauge<Integer> cacheSizeGauge;

    @SuppressWarnings("unused")
    public final Gauge<Integer> pagesInMemoryGauge;

    @SuppressWarnings("unused")
    public final Gauge<Long> pagesDeletedGauge;

    public final Histogram encodeFirstKeySize;

    public final Histogram encodeNextFirstKeySize;

    public final Histogram encodeKeySize;

    public final Histogram encodeValueSize;

    // This metrics counts compressed bytes
    public final Histogram encodePageSize;

    // This metrics counts compressed bytes
    public final Histogram numberKeysPerPage;

    public PageCacheMetrics(AbstractPageCache<K, V> cache) {
        parent = cache;
        memEstimateGauge = Metrics.newGauge(cache.getClass(),
                "memoryEstimate", parent.scope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return parent.getMemoryEstimate();
                    }
                });

        cacheSizeGauge = Metrics.newGauge(cache.getClass(),
                "cacheSize", parent.scope,
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return parent.cacheSize.get();
                    }
                });

        pagesInMemoryGauge = Metrics.newGauge(cache.getClass(),
                "pagesInMemory", parent.scope,
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return parent.numPagesInMemory.get();
                    }
                });

        pagesDeletedGauge = Metrics.newGauge(cache.getClass(),
                "pagesDeleted", parent.scope,
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return parent.numPagesDeleted.get();
                    }
                });

        encodeFirstKeySize = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "encodeFirstKeySize", parent.scope) :
                             null;

        encodeNextFirstKeySize = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "encodeNextFirstKeySize", parent.scope) :
                                 null;

        encodeKeySize = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "encodeKeySize", parent.scope) :
                        null;

        encodeValueSize = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "encodeValueSize", parent.scope) :
                          null;

        encodePageSize = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "encodePageSize", parent.scope) :
                         null;

        numberKeysPerPage = NonConcurrentPageCache.trackEncodingByteUsage ?
                Metrics.newHistogram(cache.getClass(), "numberKeysPerPage", parent.scope) :
                            null;

    }

}

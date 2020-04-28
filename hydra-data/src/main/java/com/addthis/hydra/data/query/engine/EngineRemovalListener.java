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

package com.addthis.hydra.data.query.engine;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EngineRemovalListener implements RemovalListener<String, QueryEngine> {

    private static final Logger log = LoggerFactory.getLogger(EngineRemovalListener.class);

    /**
     * metric to track the number of evicted directories. Due to some concurrency constraints, it
     * is only a 'pretty good estimate' (it is possible to over count if a directory is refreshed
     * and then quickly evicted)
     */
    static final Meter directoriesEvicted = Metrics.newMeter(QueryEngineCache.class, "directoriesEvicted",
            "directoriesEvicted", TimeUnit.MINUTES);

    static final Meter replacedEvicted = Metrics.newMeter(QueryEngineCache.class, "ReplacedEvicted",
                                                          "ReplacedEvicted", TimeUnit.MINUTES);

    static final Meter collectedEvicted = Metrics.newMeter(QueryEngineCache.class, "CollectedEvicted",
                                                           "CollectedEvicted", TimeUnit.MINUTES);

    static final Meter expiredEvicted = Metrics.newMeter(QueryEngineCache.class, "ExpiredEvicted",
                                                         "ExpiredEvicted", TimeUnit.MINUTES);

    static final Meter sizeEvicted = Metrics.newMeter(QueryEngineCache.class, "SizeEvicted",
                                                      "SizeEvicted", TimeUnit.MINUTES);

    private final QueryEngineCache engineCache;

    public EngineRemovalListener(QueryEngineCache engineCache) {
        this.engineCache = engineCache;
    }

    @Override
    public void onRemoval(RemovalNotification<String, QueryEngine> notification) {
        QueryEngine qe = notification.getValue();
        // a refresh call that returns the current value can generate spurious events
        QueryEngine currentEngine = engineCache.loadingEngineCache.asMap().get(notification.getKey());
        if (currentEngine != qe) {
            assert qe != null; //we only use strong references
            try {
                qe.closeWhenIdle();
            } catch (Throwable t) {
                log.error("Generic Error while closing Engine", t);
            }
            if (currentEngine == null) {
                directoriesEvicted.mark();
                if(notification.getCause() == RemovalCause.REPLACED) {
                    replacedEvicted.mark();
                }
                collectedEvicted.mark();
                expiredEvicted.mark();
                sizeEvicted.mark();

            }
        }
    }
}

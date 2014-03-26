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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RefreshEngineCall implements Callable<QueryEngine> {

    private static final Logger log = LoggerFactory.getLogger(RefreshEngineCall.class);

    /**
     * try to load all tree nodes from the old engine's cache before completing an engine refresh. Provokes a lot of
     * activity and hopefully helpful cache initialization from the new page cache.
     */
    private static final boolean WARM_ON_REFRESH = Parameter.boolValue("queryEngineCache.warmOnRefresh", true);

    /**
     * metric to track the number of engines refreshed
     */
    static final Meter enginesRefreshed = Metrics.newMeter(QueryEngineCache.class, "enginesRefreshed",
            "enginesRefreshed", TimeUnit.MINUTES);

    private final String dir;
    private final QueryEngine oldValue;
    private final EngineLoader engineLoader;

    public RefreshEngineCall(String dir, QueryEngine oldValue, EngineLoader engineLoader) {
        this.dir = dir;
        this.oldValue = oldValue;
        this.engineLoader = engineLoader;
    }

    @Override
    public QueryEngine call() throws Exception {
        QueryEngine qe = engineLoader.newQueryEngineDirectory(dir);
        if (WARM_ON_REFRESH) {
            try {
                ((QueryEngineDirectory) qe).loadAllFrom((QueryEngineDirectory) oldValue);
            } catch (Exception e) {
                //should either swallow if recoverable or close and rethrow if not
                //can't think of any reason it wouldn't be recoverable, so just log
                log.warn("Swallowing exception while warming replacement engine for {}",
                        ((QueryEngineDirectory) qe).getDirectory(), e);
            }
        }
        enginesRefreshed.mark();
        return qe;
    }
}

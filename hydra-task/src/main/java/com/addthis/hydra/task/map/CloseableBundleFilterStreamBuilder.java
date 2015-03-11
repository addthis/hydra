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
package com.addthis.hydra.task.map;

import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.closeablebundle.CloseableBundleFilter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class CloseableBundleFilterStreamBuilder extends StreamBuilder {

    private static final Logger log = LoggerFactory.getLogger(CloseableBundleFilterStreamBuilder.class);

    @FieldConfig(codable = true, required = true)
    private CloseableBundleFilter cfilter;

    private final Meter filterPasses = Metrics.newMeter(getClass(), "filterPasses", "passes", TimeUnit.SECONDS);
    private final Meter filterFailures = Metrics.newMeter(getClass(), "filterFailures", "failures", TimeUnit.SECONDS);

    @Override
    public void init() {
    }


    @Override
    public void process(Bundle row, StreamEmitter emitter) {
        if (cfilter.filter(row)) {
            emitter.emit(row);
            filterPasses.mark();
        } else {
            filterFailures.mark();
        }
    }

    @Override
    public void streamComplete(StreamEmitter streamEmitter) {
        log.warn("[streamComplete] closing, filters");
        cfilter.close();
    }
}

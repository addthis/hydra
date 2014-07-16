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

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

/**
 * This builder gathers, sorts, and de-duplicates incoming bundles.
 * <p/>
 * NOTE:  this is a lossy operation.  The {@code batchSize} field is used
 * to gather n bundles into a batch.  Bundles in that batch are sorted
 * and de-duplicated based on the {@code field} provided as input to this
 * class.
 * <p/>
 * Because bundles are not emitted until the {@code batchSize} is reached
 * it is possible that the system will go into shutdown mode and fail to emit
 * up to {@code batchSize} elements that are in the map but have not yet been
 * emitted to the processor. (FIXME: StreamBuilder can now shutdown())
 */
public class SortedDeDupBuilder extends StreamBuilder {

    private final ConcurrentSkipListMap<String, Bundle> sortedMap = new ConcurrentSkipListMap<>();
    private final Counter dropCounter = Metrics.newCounter(this.getClass(), "dropCounter");
    private final Lock flushLock = new ReentrantLock();

    @FieldConfig(codable = true)
    private String field;
    @FieldConfig(codable = true)
    private int batchSize = 100;
    @FieldConfig(codable = true)
    private BundleFilter filter;

    @Override
    public void init() {
    }

    @Override
    public void process(Bundle bundle, StreamEmitter emitter) {
        if (filter == null || filter.filter(bundle)) {
            ValueObject valueObject = bundle.getValue(bundle.getFormat().getField(field));
            if (valueObject == null) {
                return;
            }
            String keyValue = valueObject.asString().toString();
            if (sortedMap.put(keyValue, bundle) != null) {
                dropCounter.inc();
            }
            if (sortedMap.size() >= batchSize) {
                if (flushLock.tryLock()) {
                    try {
                        for (Bundle sortedDeDupedBundle : sortedMap.values()) {
                            emitter.emit(sortedDeDupedBundle);
                        }
                        sortedMap.clear();
                    } finally {
                        flushLock.unlock();
                    }
                }
            }
        }
    }
}

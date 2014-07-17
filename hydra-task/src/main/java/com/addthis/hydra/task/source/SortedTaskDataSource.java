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
package com.addthis.hydra.task.source;

import java.util.Iterator;
import java.util.TreeMap;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleComparator;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.task.run.TaskRunConfig;

/**
 * This {@link TaskDataSource source} <span class="hydra-summary">sorts an underlying data source</span>.
 *
 * @user-reference
 * @hydra-name sorted
 */
public class SortedTaskDataSource extends TaskDataSource {

    /**
     * Underlying data source that will be sorted. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataSource source;

    /**
     * How to sort the underlying data source. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleComparator comparator;

    @FieldConfig(codable = true)
    private Integer elements;

    @FieldConfig(codable = true)
    private String onField;

    private final TreeMap<Bundle, Bundle> sorted;

    private ValueObject lastValue;

    public SortedTaskDataSource() {
        sorted = new TreeMap<>(comparator);
    }

    @Override public void init(TaskRunConfig config) {
        source.init(config);
    }

    private void fill() {
        if (elements != null) {
            while (sorted.size() < elements) {
                Bundle next = source.next();
                if (next == null) {
                    return;
                }
                sorted.put(next, next);
            }
        }
        if (onField != null) {
            for (;;) {
                Bundle peek = source.peek();
                if (peek == null) {
                    return;
                }
                BundleFormat format = peek.getFormat();
                BundleField onBundleField = format.getField(onField);
                ValueObject value = peek.getValue(onBundleField);
                if (value == null) {
                    lastValue = null;
                } else if (lastValue == null || value.equals(lastValue)) {
                    lastValue = value;
                    final Bundle next = source.next();
                    sorted.put(next, next);
                } else {
                    lastValue = null;
                }
            }
        }
    }

    @Override
    public Bundle next() throws DataChannelError {
        fill();
        if (sorted.size() == 0) {
            return null;
        }
        Iterator<Bundle> iter = sorted.values().iterator();
        Bundle next = iter.next();
        iter.remove();
        return next;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        fill();
        if (sorted.size() == 0) {
            return null;
        }
        return sorted.values().iterator().next();
    }

    @Override
    public void close() {
        source.close();
    }
}

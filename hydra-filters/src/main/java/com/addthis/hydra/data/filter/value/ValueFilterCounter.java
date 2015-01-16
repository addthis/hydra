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
package com.addthis.hydra.data.filter.value;

import java.util.concurrent.atomic.AtomicInteger;

import java.text.DecimalFormat;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">counts the number of values it has observed</span>.
 * <p/>
 * <p>The default behavior of this filter is to emit the count of the
 * number of values it has observed. If the {@link #sample sample} field is set
 * to a value <i>N</i>, then this filter emits every <i>N</i><sup>th</sup>
 * value that is observed (a counter is not emitted). The {@link #format format}
 * field can be used to control the output when the counter is omitted.
 * When both the {@link #sample sample} and {@link #format format} fields
 * are used, then the filter emits every <i>N</i><sup>th</sup> value of the counter.
 * <p/>
 * <p>Note: by default Hydra runs with multiple worker threads. Multiple bundles
 * may be processed concurrently (at the same time). This filter will guarantee
 * that a unique integer will be assigned to each value that is observed,
 * but it cannot guarantee that the integers will be in same order of the
 * bundles from the input source.
 * <p/>
 * <p>Examples:</p>
 * <pre>
 *   {op:"count", format:"0000000"} // emits the number of items observed thus far
 *   {op:"count", sample: 5} // emits the value of each 5th item
 *   {op:"count", sample: 5, format:"000000"} // emits the number of items for each 5th item
 * </pre>
 *
 * @user-reference
 * @hydra-name count
 */
public class ValueFilterCounter extends ValueFilter {

    private DecimalFormat dc;

    /**
     * The {@link DecimalFormat DecimalFormat} input string.
     */
    private final String format;

    /**-
     * The starting value of the counter. Default is 0.
     */
    private final int start;

    /**
     * The counter increment for each input item. Default is 1.
     */
    private final int increment;

    /**
     * If non-zero, then emit an output for each <i>N</i><sup>th</sup> item. Default is 0.
     */
    private final int sample;

    private final AtomicInteger counter = new AtomicInteger();

    @JsonCreator
    public ValueFilterCounter(@JsonProperty("format") String format,
                              @JsonProperty("start") int start,
                              @JsonProperty("increment") int increment,
                              @JsonProperty("sample") int sample) {
        this.format = format;
        this.start = start;
        this.increment = increment;
        this.sample = sample;
        counter.set(start);
    }

    @Override
    public void open() { }

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (format != null && dc == null) {
            dc = new DecimalFormat(format);
        }
        int ret = counter.getAndAdd(increment);
        if (sample > 0) {
            if (ret % sample != 0) {
                return null;
            }
            if (format == null) {
                return value;
            }
        }
        if (dc != null) {
            return ValueFactory.create(dc.format(ret));
        } else {
            return ValueFactory.create(ret);
        }
    }
}

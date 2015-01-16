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

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;


/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">provides a default value for a null input or an empty string</span>.
 * <p/>
 * <p>If {@link #time time} is set to true, then the default value is set to the current wall clock
 * in unix milliseconds using the GMT timezone.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"PAGE_DOMAIN", filter:{op:"default",value:"malformed"}}
 * </pre>
 *
 * @user-reference
 * @hydra-name default
 */
public class ValueFilterDefault extends StringFilter {

    public ValueFilterDefault setValue(String value) {
        this.value = value;
        return this;
    }

    /**
     * The value to return for a null input or an empty string.
     */
    @FieldConfig(codable = true)
    private String value;

    /**
     * If set to true, then the default value is the current wall clock in unix milliseconds.
     * Default is false.
     */
    @FieldConfig(codable = true)
    private boolean time;

    public ValueFilterDefault setDefault(String dv) {
        value = dv;
        return this;
    }

    @Override
    public void open() {}

    @Override
    public String filter(String v) {
        if (Strings.isEmpty(v)) {
            return time ? Long.toString(JitterClock.globalTime()) : value;
        }
        return v;
    }

}

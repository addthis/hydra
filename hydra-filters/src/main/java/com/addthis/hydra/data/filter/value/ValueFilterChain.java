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

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">executes a series of filters</span>.
 * <p/>
 * <p>By default the first filter to return null terminates the chain.
 * This can be overridden for the entire chain by setting {@link #nullStop nullStop}
 * to {@code false}.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"field", from:"FOO_2_BAR", to:"FOO",
 *       filter:{op:"chain", filter:[{op:"split", split:"="}, {op:"index", index:0}]}},
 * </pre>
 *
 * @user-reference
 * @hydra-name chain
 */
public class ValueFilterChain extends AbstractValueFilter {

    /** The value filters to be performed in a chain. */
    @JsonProperty(required = true)
    private ValueFilter[] filter;

    /** If true, then terminate chain on first null output. Default is true. */
    @FieldConfig(codable = true)
    private boolean nullStop = true;

    @Override
    public ValueObject filterValue(ValueObject value) {
        for (ValueFilter f : filter) {
            value = f.filter(value);
            if ((value == null) && nullStop) {
                return null;
            }
        }
        return value;
    }
}

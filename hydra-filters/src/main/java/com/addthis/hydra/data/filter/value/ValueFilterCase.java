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

import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">converts a value to lowercase or uppercase letters</span>.
 * <p/>
 * <p>Enabling <a href="#lower">lower</a> will convert the input into lowercase latters.
 * Enabling <a href="#upper">upper</a> will convert the input into uppercase letters. If both
 * lower and upper are <code>false</code> then this filter performs no operation. If both
 * lower and upper are <code>true</code> then this filter will first convert to lowercase format
 * and then convert to uppercase format, which is not recommended.
 * <p>Example:</p>
 * <pre>
 *   {op:"field", from:"PAGE_DOMAIN", filter:{op:"case",lower:true}},
 * </pre>
 *
 * @user-reference
 * @hydra-name case
 */
public class ValueFilterCase extends StringFilter {

    /**
     * If <code>true</code>, then convert to lowercase letters.
     */
    @FieldConfig(codable = true)
    private boolean lower;

    /**
     * If <code>true</code>, then convert to uppercase letters.
     */
    @FieldConfig(codable = true)
    private boolean upper;

    @Override
    public String filter(String value) {
        if (value != null) {
            if (lower) {
                value = value.toLowerCase();
            }
            if (upper) {
                value = value.toUpperCase();
            }
        }
        return value;
    }

}

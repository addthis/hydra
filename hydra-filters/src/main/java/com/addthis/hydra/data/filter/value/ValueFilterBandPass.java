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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">filters values that occur fewer than {@link #minHits minHits} times
 * or more than {@link #maxHits maxHits} times</span>.
 * <p/>
 * <p>A value is emitted only after it has been observed for {@link #minHits minHits} instances.
 * The value will continue to be emitted until it has been observed for {@link #maxHits maxHits} instances.
 * At most {@link #maxKeys maxKeys} can be tracked at any one time. Assigning {@link #maxHits maxHits}
 * a value of 0 will disable filtering on the upper bound. Assigning {@link #maxKeys maxKeys} a value
 * of 0 will disable the bound on the number of unique keys. The {@link #whitelist whitelist} can be used
 * to specify values that are never to be filtered.</p>
 * <p>Example:</p>
 * <pre>
 *   {op:"field", from:"IP", filter:{op:"band-pass", maxKeys:100000, minHits:10, maxHits:100}},
 * </pre>
 *
 * @user-reference
 */
@SuppressWarnings("serial")
public class ValueFilterBandPass extends StringFilter {

    /**
     * The minimum number of times a value is observed before it is emitted. Default is 0.
     */
    @FieldConfig(codable = true)
    private int minHits;

    /**
     * The maximum number of times a value can be observed. Default is 0 which disables the upper
     * bound.
     */
    @FieldConfig(codable = true)
    private int maxHits;

    /**
     * The maximum number of unique values that can be tracked.
     * When this threshold is exceeded, then the oldest observed value
     * resets its count information. Default is 0 which allows an unbounded number of keys.
     */
    @FieldConfig(codable = true)
    private int maxKeys;

    /**
     * Stores a set of values that are not to be filtered.
     */
    @FieldConfig(codable = true)
    private HashSet<String> whitelist;

    private LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>() {
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return maxKeys > 0 && size() > maxKeys;
        }
    };

    @Override
    public String filter(String value) {
        if (value == null) {
            return value;
        }
        if (whitelist != null && whitelist.contains(value)) {
            return value;
        }
        synchronized (map) {
            Integer i = map.get(value);
            if (i == null) {
                i = 0;
            }
            map.put(value, i + 1);
            return (minHits == 0 || i >= minHits) && (maxHits == 0 || i < maxHits) ? value : null;
        }
    }

}

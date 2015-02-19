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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">auto-generates key sequences</span>.
 * <p/>
 * <p>This filter ignores the input value. The output is an incrementing counter.
 * The {@link #seed seed} field is the starting value in the sequence.
 * The {@link #mod mod} field is the value at which the seed resets to 0.
 * The {@link #round round} field is for rounding down to 10s, 100s, 1000s, etc.
 * The {@link #prefix prefix} field is an optional string prefix on the output.
 * The output always has a string format.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"serial", seed:13, base:10}
 * </pre>
 *
 * @user-reference
 * @hydra-name serial
 */
public class ValueFilterSerial extends AbstractValueFilter {

    /**
     * If non-null, then append this prefix onto the output. Default is null.
     */
    @FieldConfig(codable = true)
    private String prefix;

    /**
     * The starting value for the key sequence.
     */
    @FieldConfig(codable = true)
    private long seed;

    /**
     * If set to a positive integer, then the output will be in the range [0, mod). Default is 0.
     */
    @FieldConfig(codable = true)
    private long mod;

    /**
     * If set to a positive integer, then the output will be rounded down to the nearest N units. Default is 0.
     */
    @FieldConfig(codable = true)
    private long round;

    /**
     * The radix for the output string.
     */
    @FieldConfig(codable = true)
    private int base;

    @Override
    public synchronized ValueObject filterValue(ValueObject value) {
        if (mod > 0) {
            seed = (seed + 1) % mod;
        } else {
            seed++;
        }
        long val = seed;
        if (round > 0) {
            val -= (val % round);
        }
        if (prefix != null) {
            return ValueFactory.create(prefix.concat(Long.toString(val, base)));
        } else {
            return ValueFactory.create(Long.toString(val, base));
        }
    }

}

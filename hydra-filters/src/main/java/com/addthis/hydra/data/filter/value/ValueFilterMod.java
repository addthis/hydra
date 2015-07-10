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

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns the modulo of the input</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {from:"SHARD", filter:[{hash {}}, {mod:%[shards:512]%}, {pad.left:"000"}]}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterMod extends AbstractValueFilter {

    /**
     * The modulus of the operation.
     */
    @FieldConfig(codable = true)
    private int mod = 1;

    /**
     * If true, then return the absolute value of the operation. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean abs = true;

    @Override
    public ValueObject filterValue(ValueObject value) {
        Numeric lv = ValueUtil.asNumber(value);
        if (lv != null) {
            long v = lv.asLong().getLong();
            if (abs) {
                v = Math.abs(v);
            }
            return ValueFactory.create(v % mod);
        }
        return value;
    }
}

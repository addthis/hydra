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
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns the modulo of the input</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * {op:"field", from:"SHARD", filter:{op:"chain", filter:[{op:"hash"},{op:"mod",mod:%[shards:512]%},{op:"pad",left:"000"}]}},
 * </pre>
 *
 * @user-reference
 * @hydra-name mod
 */
public class ValueFilterMod extends ValueFilter {

    /**
     * The modulus of the operation.
     */
    @Codec.Set(codable = true)
    private int mod = 1;

    /**
     * If true, then return the absolute value of the operation. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean abs = true;

    @Override
    public ValueObject filterValue(ValueObject value) {
        try {
            ValueNumber lv = ValueUtil.asNumber(value);
            if (lv != null) {
                long v = lv.asLong().getLong();
                if (abs) {
                    v = Math.abs(v);
                }
                return ValueFactory.create(v % mod);
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return value;
    }

}

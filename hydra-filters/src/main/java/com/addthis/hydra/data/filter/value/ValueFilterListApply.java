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

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">applies the filter argument to each element of the input list</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name list-apply
 * @exclude-fields once
 */
public class ValueFilterListApply extends ValueFilter {

    /**
     * The filter to be applied to each element of the input list.
     */
    @Codec.Set(codable = true, required = true)
    private ValueFilter elementFilter;

    @Override
    // This is a essentially a copy of the default ValueFilter.filter (which applies filterValue to list elements).
    // Reason: some filters override filter rather than filterValue, which prevents them from being applied
    // to lists.
    public ValueObject filter(ValueObject value) {
        if (value != null && value.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray in = value.asArray();
            ValueArray out = null;
            for (ValueObject vo : in) {
                ValueObject val = this.elementFilter.filter(vo);
                if (val != null) {
                    if (out == null) {
                        out = ValueFactory.createArray(in.size());
                    }
                    out.append(val);
                }
            }
            return out;
        }
        return null;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        return value;
    }
}

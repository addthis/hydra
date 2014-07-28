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
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">converts the input into an array of
 * integers</span>.
 * <p/>
 * <p>If the <i>n</i><sup>th</sup> bit is set in the bitwise representation of
 * the input, then the value <i>n</i> will be an element in the output array. The output
 * array is sorted in increasing order.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {from:"VAL", to:"BIT_ARR", filter:{op:"bit-split"}}
 * </pre>
 *
 * @user-reference
 * @hydra-name bit-split
 */
public class ValueFilterBitsToArray extends ValueFilter {

    /**
     * If non-zero, then only return the bits from the mask.
     */
    @FieldConfig(codable = true)
    private long mask;

    /**
     * If the input is a string, then use this radix to convert into a long.
     */
    @FieldConfig(codable = true)
    private int radix;

    /**
     * If non-zero, then only produce output when the input is equal to matchMask.
     */
    @FieldConfig(codable = true)
    private long matchMask;

    /**
     * If non-zero, then do not produce output when the input is equal to filterMask.
     */
    @FieldConfig(codable = true)
    private long filterMask;

    private long getBitValue(ValueObject value) {
        long ret = ValueUtil.asNumberOrParseLong(value, radix).asLong().getLong();
        if (filterMask != 0 && (ret & filterMask) == filterMask) {
            return 0;
        }
        if (matchMask != 0 && (ret & matchMask) != matchMask) {
            return ret;
        }
        return ret;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        long bits = getBitValue(value);
        ValueArray arr = ValueFactory.createArray(64);
        if (bits > 0) {
            long bit = 1;
            for (int i = 0; i < 64; i++) {
                if (mask == 0 || (mask & bit) == bit) {
                    if ((bits & bit) == bit) {
                        arr.add(ValueFactory.create(i));
                    }
                }
                bit <<= 1;
            }
        }
        return arr;
    }

}

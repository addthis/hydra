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
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns the <i>i</i><sup>th</sup> element of an array</span>.
 * <p/>
 * <p>The {@link #index index} specifies the 0-based offset of the element to retrieve.
 * A negative value for the index will retrieve the (-i) value from the opposite end of the array.
 * If {@link #toNull toNull} is true, then a null value is returned when the index is out of bounds.
 * If toNull is false, then the entire array is returned when the index is out of bounds.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"FOO_2_BAR", to:"FOO",
 *          filter: {op:"chain", filter:[{op:"split", split:"="}, {op:"index", index:0}]}}
 * </pre>
 *
 * @user-reference
 * @hydra-name index
 * @exclude-fields once
 */
public class ValueFilterIndex extends ValueFilter {

    /**
     * The array offset of the element to return.
     */
    @FieldConfig(codable = true)
    private int     index;
    /**
     * If true, then return null when the index is out of bounds. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean toNull;

    public ValueFilterIndex setIndex(int index) {
        this.index = index;
        return this;
    }

    public ValueFilterIndex setToNull(boolean toNull) {
        this.toNull = toNull;
        return this;
    }

    @Override
    public ValueObject filter(ValueObject value) {
        ValueObject nullReturn = toNull ? null : value;
        if (ValueUtil.isEmpty(value)) {
            return nullReturn;
        }
        if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray arr = value.asArray();
            if (arr.size() == 0) {
                return nullReturn;
            }
            int i = index;
            while (i < 0) {
                i = arr.size() + i;
            }
            return arr.size() > i ? arr.get(i) : nullReturn;
        }
        return nullReturn;
    }

    /**
     * required to override by contract -- not used
     */
    @Override
    public ValueObject filterValue(ValueObject value) {
        return value;
    }
}

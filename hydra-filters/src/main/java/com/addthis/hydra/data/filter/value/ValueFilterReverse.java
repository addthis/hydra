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

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">reverses a string or an array</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"TEXT", filter:{op:"reverse"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name reverse
 */
public class ValueFilterReverse extends AbstractValueFilter {

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return null;
        }
        if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray arr = value.asArray();
            int size = arr.size();
            int half = size / 2;
            for (int i = 0; i < half; i++) {
                int altpos = size - i - 1;
                ValueObject tmp = arr.get(i);
                arr.set(i, arr.get(altpos));
                arr.set(altpos, tmp);
            }
            return arr;
        } else if (value.getObjectType() == ValueObject.TYPE.STRING) {
            return ValueFactory.create(new StringBuilder(value.toString()).reverse().toString());
        }
        return value;
    }

}

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


/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns the length of the array, map or string</span>.
 * <p/>
 * <p>To get the length of an array, the {@link AbstractValueFilter#once once} field must be enabled,
 * otherwise the filter attempts to iterate over the array elements.
 * <p>Example:</p>
 * <pre>
 *
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterLength extends AbstractValueFilter {

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value != null) {
            if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
                return ValueFactory.create(value.asArray().size());
            }
            if (value.getObjectType() == ValueObject.TYPE.MAP) {
                return ValueFactory.create(value.asMap().size());
            }
            Numeric num = ValueUtil.asNumber(value);
            if (num != null) {
                return num;
            }
            return ValueFactory.create(ValueUtil.asNativeString(value).length());
        }
        return null;
    }

}

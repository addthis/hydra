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
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns the passed value if it is empty or null otherwise</span>.
 * <p/>
 * <p>By default this filter returns only empty values or null otherwise.
 * Specifying the parameter {@link #not not} as {@code true} inverts the
 * behavior of the filter; all non-empty values are returned or null otherwise.
 * Specifying {@link #not not} as {@code true} is a useful method of filtering
 * out empty values, as shown in the example below.</p>
 * <p>Empty for arrays or maps is defined as having zero elements. Empty for strings is defined
 * as having a length of zero. The {@link #not not} field can be used to return all non-empty elements.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"TEXT", filter:{op:"empty", not:true}},
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterEmpty extends AbstractValueFilter {

    /**
     * If true then return all non-empty elements. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean not;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return null;
        }
        boolean empty = false;
        switch (value.getObjectType()) {
            case ARRAY:
                empty = value.asArray().size() == 0;
                break;
            case MAP:
                empty = value.asMap().size() == 0;
                break;
            default:
                String s = ValueUtil.asNativeString(value);
                empty = s != null && s.length() == 0;
                break;
        }
        if (not) {
            return empty ? null : value;
        } else {
            return empty ? value : null;
        }
    }

}

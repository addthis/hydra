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
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">puts a value into an input map.</span>.
 * <p/>
 * <p>The input must be a map. Null is returned if the input is not a map.
 * The modified map is returned otherwise. Basically it just calls input.put(key, value)
 * where input is a {@link ValueMap ValueMap}.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {from:"MAP_FIELD", filter.map-put{key:"mykey", value:"value}}
 * </pre>
 *
 * @user-reference
 * @hydra-name map-put
 */
public class ValueFilterMapPut extends ValueFilter {
    /**
     * The map key to put
     */
    @FieldConfig(codable = true)
    private String key;

    /**
     * The value to put, replaces any previous value for this key
     */
    @FieldConfig(codable = true)
    private String value;

    @Override
    public ValueObject filterValue(ValueObject v) {
        if (v == null || v.getObjectType() != ValueObject.TYPE.MAP) {
            // TODO: log error
            return null;
        }
        ValueMap<?> mapValue = v.asMap();
        mapValue.put(key, (ValueObject) ValueFactory.create(value));
        return v;
    }
}

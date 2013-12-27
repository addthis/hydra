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

import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns the value associated with a specific key
 * from the input map</span>.
 * <p>A value of null is returned if the key is not present in the map.
 * <p>Example:</p>
 * <pre>
 *     {op: "field", from: "URL_PARAMS", to: "foo", filter: {op: "map-value", key: "foo"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name map-value
 */
public class ValueFilterMapValue extends ValueFilter {

    /**
     * The key to match from the input map.
     */
    @Codec.Set(codable = true)
    private String key;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null || value.getObjectType() != ValueObject.TYPE.MAP) {
            // TODO: log error
            return null;
        }
        ValueMap mapValue = value.asMap();
        return mapValue.get(key);
    }
}

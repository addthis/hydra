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
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">extracts a map from a string</span>.
 * <p/>
 * <p>The input string is expected to be a sequence of (key,value) pairs.
 * {@link #elementSeparator elementSeperator} is the deliminator in between
 * each (key, value) pairs. {@link #keySeparator keySeperator} is the deliminator
 * in between a key and its corresponding value. This filter will also accept
 * an array as input. Each element of the array is expected to be a string
 * that is formatted using the previously described pattern. All of the (key,value)
 * pairs in the array will be collapsed into a single map.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op: "field", from: "URL_PARAMS", filter: {op: "create-map", elementSeparator:"&", keySeparator:"="}},
 * </pre>
 *
 * @user-reference
 * @hydra-name create-map
 */
public class ValueFilterCreateMap extends ValueFilter {

    /**
     * This field is never used. Do with it what you want.
     */
    @Codec.Set(codable = true)
    private String input;

    /**
     * The deliminator between a key and a value. Default is "=" .
     */
    @Codec.Set(codable = true)
    private String keySeparator = "=";

    /**
     * The deliminator between (key,value) pairs. Default is "," .
     */
    @Codec.Set(codable = true)
    private String elementSeparator = ",";

    /**
     * If true then include null values into the map. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean includeNullValues = false;


    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value != null) {
            ValueMap map = ValueFactory.createMap();
            if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
                for (ValueObject vo : ((ValueArray) value)) {
                    // expect ValueString here
                    if (vo.getObjectType() == ValueObject.TYPE.STRING) {
                        insertMapValue(map, vo);
                    }
                }
            } else {
                insertMapValue(map, value);
            }
            return map;
        }
        return null;
    }

    private void insertMapValue(ValueMap map, ValueObject vo) {
        String valueString = vo.asString().getString();
        String[] elementArray = valueString.split(elementSeparator);
        for (String element : elementArray) {
            String[] keyValue = element.split(keySeparator);
            if (!includeNullValues && keyValue.length != 2) {
                continue;
            }
            map.put(keyValue[0], keyValue.length > 1 ? ValueFactory.create(keyValue[1]) : null);
        }
    }
}

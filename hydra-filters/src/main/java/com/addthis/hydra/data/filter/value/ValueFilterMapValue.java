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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.AutoParam;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns or modifies the value associated with a specific key
 * from the input map</span>.
 * <p>A value of null is returned if the key is not present in the map.
 * <p>Example:</p>
 * <pre>
 *     {from:"URL_PARAMS", to:"foo", map-value.key:"foo"}
 *     {from:"PARTNER_COUNTS", map-value: {key.field: partnerid, put.field: newcount}}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterMapValue extends AbstractValueFilterContextual {

    /**
     * The key to match from the input map.
     */
    @AutoParam
    private AutoField key;

    /**
     * Default value to return if key is not present in map.
     */
    @AutoParam
    private AutoField defaultValue;

    /**
     * If specified, updates the key in the map with this value and returns the resulting map.
     */
    @AutoParam
    private AutoField put;

    @Override
    public ValueObject filterValue(ValueObject value, Bundle context) {
        if (value == null || value.getObjectType() != ValueObject.TYPE.MAP) {
            // TODO: log error
            return null;
        }
        ValueMap mapValue = value.asMap();
        if(this.put != null) {
            ValueMap copy = ValueFactory.createMap();
            copy.putAll(mapValue);
            copy.put(this.key.getString(context).get(), this.put.getValue(context));
            return copy;
        } else if(this.defaultValue != null) {
            return mapValue.getOrDefault(this.key.getString(context).get(), this.defaultValue.getValue(context));
        } else {
            return mapValue.get(this.key.getString(context).get());
        }
    }
}

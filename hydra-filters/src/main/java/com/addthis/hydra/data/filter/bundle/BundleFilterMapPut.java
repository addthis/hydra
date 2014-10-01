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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.util.AutoField;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">puts a value into an input map.</span>.
 * <p/>
 * <p>The {@link #map map} field must be a map, otherwise exeptions will be thrown.
 * Returns true if the map was modified. The {@link #key key} field specifies which map key to put.
 * You can specify a {@link #field field} to set the map value from, or directly specify the {@link #value value}
 * instead. If neither is specified the value will be set to null.\
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {map-put {map:"MAP_FIELD", key:"mykey", field:"VALUE_FIELD"}}
 * </pre>
 *
 * @user-reference
 * @hydra-name map-put
 */
public class BundleFilterMapPut extends BundleFilter {

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
    
    /**
     * The field to get the value to put from
     */
    @FieldConfig(codable = true)
    private AutoField field;
    
    /**
     * The map to change
     */
    @FieldConfig(codable = true)
    private AutoField map;
    
    @Override
    public void initialize() { }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public boolean filterExec(Bundle row) {
        ValueObject mapObject = map.getValue(row);
        ValueMap<?> mapValue = mapObject.asMap();
        if(field != null) {
            mapValue.put(key, field.getValue(row));
            return true;
        } else {
            mapValue.put(key, (ValueObject)ValueFactory.create(value));
            return true;
        }
    }
}


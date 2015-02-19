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

import java.util.HashMap;
import java.util.Map.Entry;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">sets bundle values to defaults if they are null or missing</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    {op:"defaults", values:{"USER" : "defaultuser"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name defaults
 */
public class BundleFilterDefaults implements BundleFilter {

    /**
     * A mapping of bundle fields to default bundle values. This field is required.
     */
    final private HashMap<String, String> values;

    final private HashMap<AutoField, ValueString> defaults;

    @JsonCreator
    public BundleFilterDefaults(@JsonProperty("values") HashMap<String, String> values) {
        this.values = values;
        HashMap<AutoField, ValueString> map = new HashMap<>();
        for (Entry<String, String> e : values.entrySet()) {
            AutoField f = AutoField.newAutoField(e.getKey());
            ValueString v = ValueFactory.create(e.getValue());
            map.put(f, v);
        }
        this.defaults = map;
    }

    @Override
    public boolean filter(Bundle row) {
        for (Entry<AutoField, ValueString> e : defaults.entrySet()) {
            ValueObject o = e.getKey().getValue(row);
            if (o == null) {
                e.getKey().setValue(row, e.getValue());
            }
        }
        return true;
    }

}

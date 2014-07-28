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
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.annotations.FieldConfig;

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
public class BundleFilterDefaults extends BundleFilter {

    /**
     * A mapping of bundle fields to default bundle values. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private HashMap<String, String> values;

    private HashMap<BundleField, ValueString> defaults;
    private boolean initialized;

    @Override
    public void initialize() {
    }

    @Override
    public boolean filterExec(Bundle row) {
        if (!initialized) {
            HashMap<BundleField, ValueString> map = new HashMap<>();
            for (Entry<String, String> e : values.entrySet()) {
                BundleField f = row.getFormat().getField(e.getKey());
                ValueString v = ValueFactory.create(e.getValue());
                map.put(f, v);
            }
            defaults = map;
            initialized = true;
        }
        for (Entry<BundleField, ValueString> e : defaults.entrySet()) {
            ValueObject o = row.getValue(e.getKey());
            if (o == null) {
                row.setValue(e.getKey(), e.getValue());
            }
        }
        return true;
    }

}

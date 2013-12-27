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
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.value.ValueFilter;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">assigns a value to a specific bundle field</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    {op:"value", to: "USERNAME", value: "Bobby Tables"},
 * </pre>
 *
 * @user-reference
 * @hydra-name value
 */
public class BundleFilterValue extends BundleFilter {

    public BundleFilterValue setValue(String value) {
        this.value = value;
        return this;
    }

    public BundleFilterValue setToField(String field) {
        this.to = field;
        return this;
    }

    public BundleFilterValue setFilter(ValueFilter filter) {
        this.filter = filter;
        return this;
    }

    public BundleFilterValue setNullFail(boolean nullFail) {
        this.nullFail = nullFail;
        return this;
    }

    /**
     * The value to assign into a bundle field. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private String value;

    /**
     * The bundle field name for the new value.
     */
    @Codec.Set(codable = true)
    private String to;

    /**
     * Optional filter to apply on the input value.
     */
    @Codec.Set(codable = true)
    private ValueFilter filter;

    /**
     * If true then return false when the input value is null. Default is true.
     */
    @Codec.Set(codable = true)
    private boolean nullFail = true;

    private String fields[];

    @Override
    public void initialize() {
        fields = new String[]{to};
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField bound[] = getBindings(bundle, fields);
        ValueObject val = ValueFactory.create(value);
        if (filter != null) {
            val = filter.filter(val);
        }
        if (nullFail && val == null) {
            return false;
        }
        bundle.setValue(bound[0], val);
        return true;
    }
}

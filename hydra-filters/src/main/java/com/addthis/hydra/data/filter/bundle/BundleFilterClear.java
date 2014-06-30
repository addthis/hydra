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
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">deletes the value from a bundle field</span>.
 * <p/>
 * <p>If the {@link #nullFail nullFail} parameter
 * is true then the bundle filter will return false when the input
 * field is null. An optional ValueFilter can be specified using
 * the {@link #filter filter} parameter. The value filter is applied on the input
 * prior to testing for null. If 'nullFail' is true and a value filter
 * is specified then the bundle field is not cleared when the value filter
 * yields is a null value.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"clear", field:"UNNECESSARY"},
 * </pre>
 *
 * @user-reference
 * @hydra-name clear
 */
public class BundleFilterClear extends BundleFilter {

    public BundleFilterClear setField(String field) {
        this.field = field;
        return this;
    }

    public BundleFilterClear setFilter(ValueFilter filter) {
        this.filter = filter;
        return this;
    }

    public BundleFilterClear setNullFail(boolean nullFail) {
        this.nullFail = nullFail;
        return this;
    }

    /**
     * The target field for clearing the value. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String field;

    /**
     * Optionally apply a filter on the target field. The result of this filter used by nullFail
     * field. The Default is null.
     */
    @FieldConfig(codable = true)
    private ValueFilter filter;

    /**
     * If true then return false when the input is null. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean nullFail = true;

    private String fields[];

    @Override
    public void initialize() {
        fields = new String[]{field};
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField bound[] = getBindings(bundle, fields);
        ValueObject val = bundle.getValue(bound[0]);
        if (filter != null) {
            val = filter.filter(val);
        }
        if (nullFail && val == null) {
            return false;
        }
        bundle.setValue(bound[0], null);
        return true;
    }
}

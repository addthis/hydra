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
import com.addthis.bundle.util.AutoField;
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
 */
public class BundleFilterClear implements BundleFilter {

    /** The target field for clearing the value. This field is required. */
    @FieldConfig(required = true) private AutoField field;

    /**
     * Optionally apply a filter on the target field. The result of this filter used by nullFail
     * field. The Default is null.
     */
    @FieldConfig private ValueFilter filter;

    /** If true then return false when the input is null. Default is true. */
    @FieldConfig private boolean nullFail = true;

    /** If true then remove the field rather than set it to null. Default is false. */
    @FieldConfig private boolean removes = false;

    @Override
    public boolean filter(Bundle bundle) {
        ValueObject val = field.getValue(bundle);
        if (filter != null) {
            val = filter.filter(val, bundle);
        }
        if (nullFail && val == null) {
            return false;
        }
        if (removes) {
            field.removeValue(bundle);
        } else {
            field.setValue(bundle, null);
        }
        return true;
    }
}

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
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">runs a value filter on a specific field</span>.
 * <p/>
 * <p>The value filter is run on the value stored at the location of the {@link #from from}
 * field. If the {@link #to to} field is specified then the output
 * of the filter operation is stored in that field. Otherwise the
 * from field is updated with the output of the filter operation.
 * If the value filter is not specified then the value is copied
 * with no filtering.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"field", from:"PAGE_DOMAIN", filter:{op:"case",lower:true}},
 * </pre>
 *
 * @user-reference
 * @hydra-name field
 */
public class BundleFilterField implements BundleFilter {

    public BundleFilterField setNullFail(boolean nullFail) {
        this.nullFail = nullFail;
        return this;
    }

    /** The input to the value filter. If the to field is null, then store the output in this field. */
    @FieldConfig(required = true) private AutoField from;

    /** The destination field for the output of the value filter. Optional field. */
    @FieldConfig private AutoField to;

    /** The filter to perform. Optional field. */
    @FieldConfig private ValueFilter filter;

    /**
     * If true then do not assign the value filter when the output is null and return the value
     * of the {@link #not not} field. Default is true.
     */
    @FieldConfig private boolean nullFail = true;

    /** The value to return when nullFail is true and the value filter output is null. Default is false. */
    @FieldConfig private boolean not;

    @Override
    public boolean filter(Bundle row) {
        ValueObject val = from.getValue(row);
        if (filter != null) {
            val = filter.filter(val, row);
        }
        if (nullFail && (val == null)) {
            return not;
        }
        if (to == null) {
            from.setValue(row, val);
        } else {
            to.setValue(row, val);
        }
        return !not;
    }
}

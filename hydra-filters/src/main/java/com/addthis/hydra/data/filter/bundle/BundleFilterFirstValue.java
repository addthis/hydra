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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.CachingField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">selects the first non-empty value from an array</span>.
 * <p/>
 * <p>The {@link #in in} field contains an array of bundle field names that are searched in order.
 * The first bundle field that contains a non-empty element is populated into the
 * location specified by the {@link #out out} field. If a non-empty element is found then
 * the filter returns true. Otherwise it returns false.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"first", in:["USER1", "USER2", "USER3"]" out:"USER_VALUE"},
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterFirstValue implements BundleFilter {

    /**
     * An array of bundle field names to search. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private CachingField[] in;

    /**
     * Output destination field. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private AutoField out;

    /**
     * Target field that will be populated by the name of the selected field. This field is
     * optional.
     */
    @FieldConfig(codable = true)
    private AutoField which;

    @Override
    public boolean filter(Bundle bundle) {
        for (int i = 0; i < in.length; i++) {
            ValueObject v = in[i].getValue(bundle);
            if (v == null) {
                continue;
            }
            switch (v.getObjectType()) {
                case ARRAY:
                    ValueArray arr = v.asArray();
                    if (arr.size() > 0) {
                        if (which != null) {
                            ValueObject fieldName = ValueFactory.create(in[i].name);
                            which.setValue(bundle, fieldName);
                        }
                        out.setValue(bundle, arr);
                        return true;
                    }
                    break;
                default:
                    String str = ValueUtil.asNativeString(v);
                    if (!Strings.isEmpty(str)) {
                        if (which != null) {
                            ValueObject fieldName = ValueFactory.create(in[i].name);
                            which.setValue(bundle, fieldName);
                        }
                        out.setValue(bundle, v);
                        return true;
                    }
                    break;
            }
        }
        return false;
    }

}

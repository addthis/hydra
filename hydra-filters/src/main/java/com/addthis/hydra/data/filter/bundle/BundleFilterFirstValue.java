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
 * @hydra-name first
 */
public class BundleFilterFirstValue extends BundleFilter {

    /**
     * An array of bundle field names to search. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] in;

    /**
     * Output destination field. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String out;

    /**
     * Target field that will be populated by the name of the selected field. This field is
     * optional.
     */
    @FieldConfig(codable = true)
    private String which;

    private String[] fields;

    public void setIn(String[] in) {
        this.in = in;
    }

    public void setOut(String out) {
        this.out = out;
    }

    public void setWhich(String which) {
        this.which = which;
    }

    public String[] getFields() {
        return fields;
    }

    @Override
    public void initialize() {
        if (which == null) {
            fields = new String[in.length + 1];
        } else {
            fields = new String[in.length + 2];
            fields[in.length + 1] = which;
        }
        System.arraycopy(in, 0, fields, 0, in.length);
        fields[in.length] = out;
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField[] bound = getBindings(bundle, fields);
        int end = (which == null) ? (bound.length - 1) : (bound.length - 2);
        for (int i = 0; i < end; i++) {
            ValueObject v = bundle.getValue(bound[i]);
            if (v == null) {
                continue;
            }
            switch (v.getObjectType()) {
                case ARRAY:
                    ValueArray arr = v.asArray();
                    if (arr.size() > 0) {
                        if (which != null) {
                            ValueObject fieldName = ValueFactory.create(bound[i].getName());
                            bundle.setValue(bound[end + 1], fieldName);
                        }
                        bundle.setValue(bound[end], arr);
                        return true;
                    }
                    break;
                default:
                    String str = ValueUtil.asNativeString(v);
                    if (!Strings.isEmpty(str)) {
                        if (which != null) {
                            ValueObject fieldName = ValueFactory.create(bound[i].getName());
                            bundle.setValue(bound[end + 1], fieldName);
                        }
                        bundle.setValue(bound[end], v);
                        return true;
                    }
                    break;
            }
        }
        return false;
    }

}

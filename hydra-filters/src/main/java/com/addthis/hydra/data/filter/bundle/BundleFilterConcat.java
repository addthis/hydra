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
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">concatenates one or more fields in a bundle</span>.
 * <p/>
 * <p>The output of this bundle filter is a string that is comprised of the string
 * concatenation of the input bundle fields.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"concat", in:["FOO", "BAR", "BAZ"], out: "OUTPUT", join: ":"},
 * </pre>
 *
 * @user-reference
 * @hydra-name concat
 */
public class BundleFilterConcat extends BundleFilter {

    /**
     * An array of fields to concatenate. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String in[];

    /**
     * The destination for the output string. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String out;

    /**
     * An optional separator to place in between elements of the output string.
     */
    @FieldConfig(codable = true)
    private String join;

    private String fields[];

    public void setIn(String[] in) {
        this.in = in;
    }

    public void setOut(String out) {
        this.out = out;
    }

    public void setJoin(String join) {
        this.join = join;
    }

    public String[] getFields() {
        return fields;
    }

    @Override
    public void initialize() {
        fields = new String[in.length + 1];
        System.arraycopy(in, 0, fields, 0, in.length);
        fields[in.length] = out;
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField bound[] = getBindings(bundle, fields);
        StringBuilder sb = new StringBuilder();
        int end = bound.length - 1;
        for (int i = 0; i < end; i++) {
            if (i > 0 && join != null) {
                sb.append(join);
            }
            ValueObject v = bundle.getValue(bound[i]);
            if (v != null && v.getObjectType().equals(ValueObject.TYPE.ARRAY)) {
                ValueArray vArray = v.asArray();
                int count = 0;
                for (ValueObject valueObject : vArray) {
                    if (count++ > 0) {
                        sb.append(join);
                    }
                    sb.append(ValueUtil.asNativeString(valueObject));
                }
            } else {
                sb.append(ValueUtil.asNativeString(v));
            }
        }
        bundle.setValue(bound[end], ValueFactory.create(sb.toString()));
        return true;
    }

}

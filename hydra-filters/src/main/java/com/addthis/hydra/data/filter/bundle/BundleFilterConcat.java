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

    /** An array of fields to concatenate. This field is required. */
    @FieldConfig(required = true) private AutoField[] in;

    /** The destination for the output string. This field is required. */
    @FieldConfig(required = true) private AutoField out;

    /** An optional separator to place in between elements of the output string. */
    @FieldConfig private String join;

    @Override
    public void initialize() { }

    @Override
    public boolean filterExec(Bundle bundle) {
        StringBuilder sb = new StringBuilder();
        boolean appendJoin = false;
        for (AutoField field : in) {
            if (appendJoin && (join != null)) {
                sb.append(join);
            }
            appendJoin = true;
            ValueObject v = field.getValue(bundle);
            if ((v != null) && v.getObjectType().equals(ValueObject.TYPE.ARRAY)) {
                ValueArray vArray = v.asArray();
                boolean innerAppendJoin = false;
                for (ValueObject valueObject : vArray) {
                    if (innerAppendJoin && (join != null)) {
                        sb.append(join);
                    }
                    innerAppendJoin = true;
                    sb.append(ValueUtil.asNativeString(valueObject));
                }
            } else {
                sb.append(ValueUtil.asNativeString(v));
            }
        }
        out.setValue(bundle, ValueFactory.create(sb.toString()));
        return true;
    }

}

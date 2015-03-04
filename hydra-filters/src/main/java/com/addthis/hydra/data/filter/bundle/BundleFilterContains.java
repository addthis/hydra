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
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.filter.value.ValueFilterContains;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This {@link com.addthis.hydra.data.filter.bundle.BundleFilter BundleFilter} <span class="hydra-summary">tests if the input contains the target value</span>.
 * <p/>
 * <p>The input is the contents of the bundle field specified by {@link #field field}.
 * If the input is a map then only the values of the map are tested.
 * The target can either be a set of strings (in the {@link #value value} field) or
 * a value from another field (in the {@link #from from} field). The filter returns true
 * if the source field contains any of the test values as a substring.
 * Otherwise the filter returns false. The filter does not modify the bundle.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"contains", field:"SRC", value:["foo", "bar"]},
 *     {op:"contains", field:"SRC", from:"TEST"]},
 *  </pre>
 *
 * @user-reference
 */
public class BundleFilterContains implements BundleFilter {

    /**
     * The input field to test. This field is required.
     */
    final private AutoField field;

    /**
     * An array of strings to test against the input field.
     */
    final private String[] value;

    /**
     * The target field to test against the input field.
     */
    final private AutoField from;

    /**
     * If true then return the negation of the contains operation. Default is false.
     */
    final private boolean not;

    // Cache the value filter if-and-only-if the 'from' field is null.
    final private ValueFilterContains filter;

    @JsonCreator
    public BundleFilterContains(@JsonProperty("field") AutoField field,
                                @JsonProperty("value") String[] value,
                                @JsonProperty("from") AutoField from,
                                @JsonProperty("not") boolean not) {
        this.field = field;
        this.value = value;
        this.from = from;
        this.not = not;
        if (from == null && value != null) {
            filter = new ValueFilterContains(value, null, false, false);
        } else {
            filter = null;
        }
    }

    @Override
    public boolean filter(Bundle row) {
        if (row == null) {
            return not;
        }
        ValueObject target = field.getValue(row);
        if (from != null) {
            String fieldString = target.asString().asNative();
            String fromString = ValueUtil.asNativeString(from.getValue(row));
            boolean match = fieldString.contains(fromString);
            return not ? !match : match;
        } else if (filter != null) {
            boolean match = filter.filterValue(target) != null;
            return not ? !match : match;
        } else {
            return not;
        }
    }

}

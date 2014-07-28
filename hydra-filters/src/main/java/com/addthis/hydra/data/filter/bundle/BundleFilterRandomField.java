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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">randomly selects an input field and copies it to an output field</span>.
 * <p/>
 * <p>The possible input fields are specified with {@link #inFields inFields}. Of the possible
 * input fields only those with non-null values are considered. One of the eligible values
 * is randomly selected and copied into the output field.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"random-field", inFields: ["INPUT1", "INPUT2", "INPUT3"], out: "OUTPUT"},
 * </pre>
 *
 * @user-reference
 * @hydra-name random-field
 */
public class BundleFilterRandomField extends BundleFilter {

    /**
     * The possible input bundle fields from which one will be selected. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] inFields;
    /**
     * The name of the output bundle field. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String out;

    public BundleFilterRandomField() {
    }

    public BundleFilterRandomField(String[] inFields, String out) {
        this.inFields = inFields;
        this.out = out;
    }

    private String[] fields;

    public String[] getFields() {
        return fields;
    }

    @Override
    public void initialize() {
        fields = new String[inFields.length + 1];
        System.arraycopy(inFields, 0, fields, 0, inFields.length);
        fields[inFields.length] = out;
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField[] bound = getBindings(bundle, fields);
        BundleField[] inBound = new BundleField[inFields.length];
        System.arraycopy(bound, 0, inBound, 0, inBound.length);
        List<BundleField> inBoundShuffle = new ArrayList<>(Arrays.asList(inBound));
        Collections.shuffle(inBoundShuffle);

        for (BundleField bf : inBoundShuffle) {
            if (bf != null && bundle.getValue(bf) != null) {
                bundle.setValue(bound[bound.length - 1], bundle.getValue(bf));
                break;
            }
        }

        return true;
    }
}

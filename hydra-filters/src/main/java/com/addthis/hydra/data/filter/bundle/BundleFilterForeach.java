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
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">applies a bundle filter over an array</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *  {op:"foreach", filter: {op:concat, in:[ OUTPUT, ELEMENT ], out: OUTPUT, join: ":"},
 *      source : SOURCE, value : ELEMENT}
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterForeach implements BundleFilter {

    /**
     * The filter to execute. This field is required.
     */
    private final BundleFilter filter;

    /**
     * The name of the field containing the array. This field is required.
     */
    private final AutoField source;

    /**
     * Optionally the name of the field that will be populated with the current index.
     */
    private final AutoField index;

    /**
     * Optionally the name of the field that will be populated with the current value.
     */
    private final AutoField value;

    /**
     * If true then terminate when an iteration returns false.
     * Default value is true.
     */
    private final boolean exitOnFailure;

    @JsonCreator
    public BundleFilterForeach(@JsonProperty(value = "filter", required = true) BundleFilter filter,
                               @JsonProperty(value = "source", required = true) AutoField source,
                               @JsonProperty("index") AutoField index,
                               @JsonProperty("value") AutoField value,
                               @JsonProperty("exitOnFailure") boolean exitOnFailure) {
        this.filter = filter;
        this.source = source;
        this.index = index;
        this.value = value;
        this.exitOnFailure = exitOnFailure;
    }

    @Override public boolean filter(Bundle row) {
        boolean success = true;
        ValueObject object = source.getValue(row);
        if (object == null) {
            return false;
        }
        ValueArray array = object.asArray();
        for (int i = 0; i < array.size(); i++) {
            if (index != null) {
                index.setValue(row, ValueFactory.create(i));
            }
            if (value != null) {
                value.setValue(row, array.get(i));
            }
            success &= filter.filter(row);
            if (!success && exitOnFailure) {
                return false;
            }
        }
        if (index != null) {
            index.setValue(row, ValueFactory.create(array.size()));
        }
        return success;
    }
}

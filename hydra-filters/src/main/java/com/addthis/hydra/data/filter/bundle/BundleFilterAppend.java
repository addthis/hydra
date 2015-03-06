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
import java.util.HashSet;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">appends a value to an array</span>.
 * <p/>
 * <p>Add the injected value to a {@link ValueArray ValueArray} if the
 * array already exists or it creates a new array with the provided value if the
 * array does not already exist.
 * <p/>
 * <p>The {@link #to to} field is required and it specifies both the start of the
 * input array and the output destination. Next any values in the {@link #values values}
 * field are appended to the array, and in the final step any values in the {@link #from from}
 * field are appended to the array. The output is stored in the {@link #to to} field.
 * <p/>
 * <p>NOTE: if the {@link #to to} or {@link #from from} fields has an existing value
 * that is not a ValueArray then this filter will convert the field
 * to a ValueArray.
 * <p>Example:</p>
 * <pre>
 *   {op:"append", to:"INPUT", values:{"a", "b", "c"}},
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterAppend implements BundleFilter {

    @JsonIgnore
    public BundleFilterAppend setValues(ArrayList<String> values) {
        this.values = values;
        return this;
    }

    @JsonIgnore
    public BundleFilterAppend setValues(HashSet<String> values) {
        this.values = new ArrayList<>(values.size());
        for (String val : values) {
            this.values.add(val);
        }
        return this;
    }

    public BundleFilterAppend setToField(AutoField field) {
        this.to = field;
        return this;
    }

    public BundleFilterAppend setFilter(ValueFilter filter) {
        this.filter = filter;
        return this;
    }

    public BundleFilterAppend setNullFail(boolean nullFail) {
        this.nullFail = nullFail;
        return this;
    }

    public BundleFilterAppend setSize(int size) {
        this.size = size;
        return this;
    }

    /**
     * Both the start of the array and the destination field. Required field.
     */
    @FieldConfig(required = true)
    private AutoField to;

    /**
     * A field to append at the end of the array.
     */
    @FieldConfig(codable = true)
    private AutoField from;

    /**
     * A list of values to append in between the 'to' and 'from' fields.
     */
    @FieldConfig(codable = true)
    private ArrayList<String> values;

    /**
     * An optional filter to apply on the 'values' field.
     */
    @FieldConfig(codable = true)
    private ValueFilter filter;

    /**
     * If true, then return a value of false if the 'to' field is null. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean nullFail;

    /**
     * If true, then append only values that are not already a members of the array. Default is
     * false.
     */
    @FieldConfig(codable = true)
    private boolean unique;

    /**
     * Optional param to hint at expected array size. Default is 5.
     */
    @FieldConfig(codable = true)
    private int size = 5;

    @Override
    public boolean filter(Bundle bundle) {
        ValueObject toVal = to.getValue(bundle);
        ValueArray arr = null;
        if (toVal == null) {
            if (nullFail) {
                return false;
            }
            arr = ValueFactory.createArray(size);
        } else {
            if (toVal.getObjectType() == ValueObject.TYPE.ARRAY) {
                arr = toVal.asArray();
            } else {
                arr = ValueUtil.asArray(toVal);
            }
        }
        // append constants
        if (values != null) {
            for (String value : values) {
                ValueString str = ValueFactory.create(value);
                if (!unique || !arr.contains(str)) {
                    arr.add(str);
                }
            }
        }
        // append from if set
        if (from != null) {
            ValueObject fromVal = from.getValue(bundle);
            if (filter != null) {
                fromVal = filter.filter(fromVal, bundle);
            }
            if (fromVal != null) {
                if (fromVal.getObjectType() == ValueObject.TYPE.ARRAY) {
                    for (ValueObject element : fromVal.asArray()) {
                        if (!unique || !arr.contains(element)) {
                            arr.add(element);
                        }
                    }
                } else if (!unique || !arr.contains(fromVal)) {
                    arr.add(fromVal);
                }
            }
        }
        to.setValue(bundle, arr);
        return true;
    }
}

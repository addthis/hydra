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
package com.addthis.hydra.task.map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.CachingField;
import com.addthis.bundle.util.FullAutoField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.filter.value.ValueFilter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This section specifies how fields of the input source are transformed into a mapped bundle.
 * <p/>
 * <p>Fields are moved from a specified field in the job {@link StreamMapper#source source}
 * to a destination field in the mapped bundle. By default null values are not written into
 * the mapped bundle. This behavior can be changed by setting the toNull field to true.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>fields:[
 *    "TIME"
 *    "SOURCE"
 *    {from:"INITIAL_NAME", to:"NEW_NAME"}
 * ]</pre>
 *
 * @user-reference
 */
public final class FieldFilter {

    /** The name of the bundle field source. This is required. */
    final AutoField from;

    /** The name of the bundle field destination. */
    final AutoField to;

    /** Optionally apply a filter onto the field. */
    @JsonProperty ValueFilter filter;

    /** If true then emit null values to the destination field. The default is false. */
    @JsonProperty boolean toNull;
    
    // Default constructor required for codec deserialization
    @SuppressWarnings("unused")
    private FieldFilter(@JsonProperty(value = "from", required = true) AutoField from,
                        @JsonProperty(value = "to") AutoField to) {
        this.from = from;
        if (to == null) {
            this.to = cloneFrom(from);
        } else {
            this.to = to;
        }
    }

    // can't find a good way to copy the json value for "from", copy constructors fail for abstract types,
    // and clone has its own host of problems. We could just re-use the same object, but that would be even
    // more wasteful than the caching we perform for unchanging formats. This is a decent stop-gap solution.
    private static AutoField cloneFrom(AutoField from) {
        if (from instanceof FullAutoField) {
            return new FullAutoField(((FullAutoField) from).baseAutoField, ((FullAutoField) from).subNames);
        } else if (from instanceof CachingField) {
            return new CachingField(((CachingField) from).name);
        } else {
            throw new IllegalArgumentException("can not use implicit relation (from = to) for AutoField type: "
                                               + from.getClass());
        }
    }

    public FieldFilter(String copyFieldName) {
        this.from = AutoField.newAutoField(copyFieldName);
        this.to   = AutoField.newAutoField(copyFieldName);
    }

    public void mapField(Bundle in, Bundle out) {
        ValueObject inVal = from.getValue(in);
        if (filter != null) {
            inVal = filter.filter(inVal, in);
        }
        if (inVal != null || toNull) {
            to.setValue(out, inVal);
        }
    }
}

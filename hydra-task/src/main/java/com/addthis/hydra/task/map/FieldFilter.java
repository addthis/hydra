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
 *    {from:"TIME", to:"TIME"},
 *    {from:"SOURCE", to:"SOURCE"},
 * ]</pre>
 *
 * @user-reference
 */
public final class FieldFilter {

    /** The name of the bundle field source. This is required. */
    @JsonProperty(required = true) AutoField from;

    /** The name of the bundle field destination. */
    @JsonProperty(required = true) AutoField to;

    /** Optionally apply a filter onto the field. */
    @JsonProperty ValueFilter filter;

    /** If true then emit null values to the destination field. The default is false. */
    @JsonProperty boolean toNull;

    public FieldFilter(String copyFieldName) {
        this.from = CachingField.newAutoField(copyFieldName);
        this.to   = CachingField.newAutoField(copyFieldName);
    }

    public void mapField(Bundle in, Bundle out) {
        ValueObject inVal = from.getValue(in);
        if (filter != null) {
            inVal = filter.filter(inVal);
        }
        if (inVal != null || toNull) {
            to.setValue(out, inVal);
        }
    }
}

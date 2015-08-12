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
package com.addthis.hydra.data.filter.value;

import javax.annotation.Nullable;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A value filter applies a transformation on a value and returns
 * the result of the transformation.
 *
 * @user-reference
 * @hydra-category Value Filters
 * @hydra-doc-position 5
 * @exclude-fields once, nullAccept
 */
public abstract class AbstractValueFilter implements ValueFilter {

    /**
     * Disables special {@link ValueArray} handling logic. By default (false), it will map over elements
     * of an array. When this flag is turned on, filters will try to process the array as a whole.
     * Default is false.
     */
    @JsonProperty protected boolean once;

    public boolean getOnce() {
        return once;
    }

    public ValueFilter setOnce(boolean o) {
        once = o;
        return this;
    }

    /**
     * Iterate over the input value and apply the filter to each element of the array.
     *
     * @param value    input array
     * @param context  Optional context for this filter.
     * @return output array
     */
    @Nullable private ValueObject filterArray(ValueObject value, Bundle context) {
        ValueArray in = value.asArray();
        ValueArray out = null;
        for (ValueObject vo : in) {
            ValueObject val = filterValue(vo, context);
            if (val != null) {
                if (out == null) {
                    out = ValueFactory.createArray(in.size());
                }
                out.add(val);
            }
        }
        return out;
    }

    /**
     * Optional variant of {@link #filter(ValueObject)} that includes context for the value. Implementations should
     * not attempt to modify the bundle provided for contextual information, and this may result in exceptions or
     * other undefined behavior. If not-overridden in a subclass then {@link AbstractValueFilter#filter(ValueObject)}
     * is called without the bundle context.
     */
    @Override @Nullable public ValueObject filter(@Nullable ValueObject value, @Nullable Bundle context) {
        return filter(value);
    }

    /**
     * Wrapper method for {@link #filterValue(ValueObject)} that has special logic for {@link ValueArray}s.
     * This should be the primary method to be called in most circumstances, and should not be overridden unless
     * special, different array handling logic is needed. When {@link #once} is true, or when the ValueObject
     * is not an array, this is the same as directly calling {@link #filterValue(ValueObject)}.
     */
    @Override @Nullable public ValueObject filter(@Nullable ValueObject value) {
        return filterWithArrayHandling(value, null);
    }

    /**
     * Helper method for {@link #filter(ValueObject)} and {@link AbstractValueFilterContextual#filter(ValueObject, Bundle)}
     * that determines if array handling is necessary.
     *
     * @param value input value
     * @param context optional bundle context
     * @return output value
     */
    @Nullable protected final ValueObject filterWithArrayHandling(@Nullable ValueObject value, @Nullable Bundle context) {
        if (once) {
            return filterValue(value, context);
        }
        // TODO why is this behaviour not there for TYPE.MAPS ?
        if ((value != null) && (value.getObjectType() == ValueObject.TYPE.ARRAY)) {
            return filterArray(value, context);
        }
        return filterValue(value, context);
    }

    /**
     * Optional variant of {@link #filterValue(ValueObject)} that includes context for the value.
     * Implementations should not attempt to modify the bundle provided for contextual information,
     * and this may result in exceptions or other undefined behavior.
     */
    @Nullable public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
        return filterValue(value);
    }

    /**
     * Accepts a value as input and returns a value as output.
     *
     * Implementers of {@link ValueFilter} are strongly discouraged
     * from modifying the state of the input value in cases where the
     * value object is mutable.
     *
     * @param value input value. Possibly null.
     * @return output value. Possibly null.
     */
    @Nullable public abstract ValueObject filterValue(@Nullable ValueObject value);
}

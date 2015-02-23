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
import com.addthis.bundle.value.ValueObject;

/**
 * This class is provided as a convenience for
 * value filters that optionally support bundle context.
 **/
public abstract class AbstractValueFilterContextual extends AbstractValueFilter {

    /**
     * Optional variant of {@link #filter(ValueObject)} that includes context for the value. Implementations should
     * not attempt to modify the bundle provided for contextual information, and this may result in exceptions or
     * other undefined behavior. If not-overridden in a subclass then {@link AbstractValueFilter#filter(ValueObject)}
     * is called without the bundle context.
     */
    @Override @Nullable public ValueObject filter(@Nullable ValueObject value, @Nullable Bundle context) {
        return filterWithArrayHandling(value, context);
    }

    /**
     * Wrapper method for {@link #filterValue(ValueObject)} that has special logic for {@link ValueArray}s.
     * This should be the primary method to be called in most circumstances, and should not be overridden unless
     * special, different array handling logic is needed. When {@link #once} is true, or when the ValueObject
     * is not an array, this is the same as directly calling {@link #filterValue(ValueObject)}.
     */
    @Override @Nullable public final ValueObject filter(@Nullable ValueObject value) {
        return filter(value, null);
    }


    /**
     * Optional variant of {@link #filterValue(ValueObject)} that includes context for the value.
     * Implementations should not attempt to modify the bundle provided for contextual information,
     * and this may result in exceptions or other undefined behavior.
     */
    @Override @Nullable public abstract ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context);

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
    @Override @Nullable public final ValueObject filterValue(@Nullable ValueObject value) {
        return filterValue(value, null);
    }
}

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
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonCreator;


/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">negates another value filter</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name not
 */
public class ValueFilterNot extends AbstractValueFilterContextual {

    private final ValueFilter filter;

    @JsonCreator public ValueFilterNot(ValueFilter filter) {
        this.filter = filter;
    }

    @Override @Nullable public ValueObject filter(@Nullable ValueObject value, @Nullable Bundle context) {
        if (filter.filter(value, context) == null) {
            return value;
        } else {
            return null;
        }
    }

    @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
        return filter.filter(value, context);
    }
}

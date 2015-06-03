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

import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">computes a value once</span>.
 *
 * <p>This value filter has one argument which is the value filter that will be used
 * to compute a value. The result of the inner value filter is cached and that
 * value is returned on subsequent computations. The inner value filter is called
 * at least once but is not guaranteed to be called exactly once.</p>
 *
 * @user-reference
 */
public class ValueFilterCache implements ValueFilter {

    private final ValueFilter filter;

    private volatile ValueObject result;

    private static final ValueObject UNINITIALIZED = new ValueObject() {
        @Override public TYPE getObjectType() {
            return null;
        }

        @Override public Object asNative() {
            return null;
        }
    };

    @JsonCreator
    public ValueFilterCache(@JsonProperty(value = "filter", required = true) ValueFilter filter) {
        this.filter = filter;
        this.result = UNINITIALIZED;
    }

    @Nullable @Override
    public ValueObject filter(@Nullable ValueObject value) {
        if (result == UNINITIALIZED) {
            result = filter.filter(value);
        }
        return result;
    }
}

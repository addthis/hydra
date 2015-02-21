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

import java.util.function.UnaryOperator;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Pluggable;

@FunctionalInterface
@Pluggable("value-filter")
public interface ValueFilter extends UnaryOperator<ValueObject> {

    @Nullable @Override default ValueObject apply(ValueObject t) {
        return filter(t);
    }

    /**
     * Optional variant of {@link #filter(ValueObject)} that includes context for the value. Implementations should
     * not attempt to modify the bundle provided for contextual information, and this may result in exceptions or
     * other undefined behavior.
     */
    @Nullable default ValueObject filter(@Nullable ValueObject value, @Nullable Bundle context) {
        return filter(value);
    }

    @Nullable ValueObject filter(@Nullable ValueObject value);
}

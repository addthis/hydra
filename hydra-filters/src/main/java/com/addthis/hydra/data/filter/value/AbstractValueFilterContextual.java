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

/** This class is provided as a convenience for value filters that optionally support bundle context. */
public abstract class AbstractValueFilterContextual extends AbstractValueFilter {

    @Override @Nullable public ValueObject filter(@Nullable ValueObject value, @Nullable Bundle context) {
        return filterWithArrayHandling(value, context);
    }

    @Override @Nullable public final ValueObject filter(@Nullable ValueObject value) {
        return filter(value, null);
    }

    @Override @Nullable public abstract ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context);

    @Override @Nullable public final ValueObject filterValue(@Nullable ValueObject value) {
        return filterValue(value, null);
    }
}

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

import com.addthis.bundle.value.ValueObject;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns the input value</span>.
 * <p/>
 * <p>No transformations are performed in the input value. It is passed along as the output value.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"STR", to:”STR2”,  filter:{op:"pass"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name pass
 * @exclude-fields once
 */
public class ValueFilterPass extends ValueFilter {

    @Override public void open() { }

    @Override
    public ValueObject filter(ValueObject v) {
        return v;
    }

    @Override
    public ValueObject filterValue(ValueObject v) {
        return v;
    }
}

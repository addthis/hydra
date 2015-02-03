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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.common.hash.MD5HashFunction;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns the MD5 sum of the input</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op: "field", from: "URL", to: "HASH", filter: {op: "md5"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name md5
 */
public class ValueFilterMD5 extends ValueFilter {

    @Override public void open() { }

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value != null) {
            value = ValueFactory.create(MD5HashFunction.hashAsString(value.toString()));
        }
        return value;
    }
}

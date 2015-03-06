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

import java.io.IOException;
import java.io.UncheckedIOException;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">parses JSON into a value object</span>.
 * <p/>
 * <p>
 * <p>Example:</p>
 * <pre>
 *       {op: "field", from: "JSON", to: "VALUE", filter: {op:"json"}},
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterJSON extends AbstractValueFilter {

    @Nullable @Override
    public ValueObject filterValue(@Nullable ValueObject value) {
        try {
            if (value == null) {
                return null;
            } else {
                return ValueFactory.decodeValue(value.asString().asNative());
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}

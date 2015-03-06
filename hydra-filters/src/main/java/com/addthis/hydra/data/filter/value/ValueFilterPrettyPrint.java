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

import java.io.UncheckedIOException;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.jackson.Jackson;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">pretty prints the value as a string</span>.
 * <p>Values that are arrays and maps do not follow the traditional value filter iterating behavior. Instead
 * they are printed as one object.</p>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"FOO", to:”BAR”,  filter:{op:"pretty-print"}},
 * </pre>
 *
 * @user-reference
 * @exclude-fields once
 */
public class ValueFilterPrettyPrint extends AbstractValueFilter {

    @Override
    public ValueObject filter(ValueObject v) {
        return ValueFactory.create(prettyPrint(v));
    }

    @Override
    public ValueObject filterValue(ValueObject v) {
        return ValueFactory.create(prettyPrint(v));
    }

    public static String prettyPrint(ValueObject input) {
        try {
            if (input == null) {
                return null;
            } else {
                return Jackson.defaultMapper().writeValueAsString(input.asNative());
            }
        } catch (JsonProcessingException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}

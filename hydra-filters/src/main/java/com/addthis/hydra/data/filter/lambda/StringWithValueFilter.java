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
package com.addthis.hydra.data.filter.lambda;

import javax.annotation.Nullable;

import java.util.function.Supplier;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.filter.value.ValueFilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Supplies a string result. The input parameter is required and the
 * filter parameter is optional. If no filter is specified then
 * the input is returned as the output. If a filter is specified the
 * the filter is applied to the input. If no filter is used then
 * the input can be provided all by itself as a string without
 * the field declaration (see below).
 *
 * <p>Examples:</p>
 * <pre>
 *     field1: "foo bar"
 *     field2: {input: "foo", filter: {op:"cat", post: " bar"}}
 * </pre>
 * @user-reference
 */
public class StringWithValueFilter implements Supplier<String>  {

    @Nullable
    private final String result;

    @JsonCreator
    public StringWithValueFilter(@Nullable final String input) {
        this(input, null);
    }

    @JsonCreator
    public StringWithValueFilter(@JsonProperty("input") @Nullable final String input,
                                 @JsonProperty("filter") @Nullable final ValueFilter filter) {
        if (filter == null) {
            result = input;
        } else {
            ValueObject object = filter.filter(ValueFactory.create(input));
            result = (object != null) ? object.asString().asNative() : null;
        }
    }

    @Override public String get() {
        return result;
    }

}

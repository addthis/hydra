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

import java.io.IOException;

import java.util.function.Supplier;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.data.filter.value.ValueFilter;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

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
@JsonDeserialize(using = StringWithValueFilter.Deserializer.class)
public class StringWithValueFilter implements Supplier<String>  {

    @Nullable
    private final String result;

    public StringWithValueFilter(@Nullable final String input, @Nullable final ValueFilter filter) {
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

    static class Deserializer extends JsonDeserializer<StringWithValueFilter> {

        @Override
        public StringWithValueFilter deserialize(JsonParser jsonParser,
                                                 DeserializationContext deserializationContext)
                throws IOException {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);
            if (node.isTextual()) {
                return new StringWithValueFilter(node.asText(), null);
            } else {
                String input = node.get("input").asText();
                JsonNode filterNode = node.get("filter");
                ValueFilter filter = null;
                if (filterNode != null) {
                    try {
                        filter = Jackson.defaultCodec().getObjectMapper()
                                        .treeToValue(filterNode, ValueFilter.class);
                    } catch (Exception ex) {
                        throw Throwables.propagate(ex);
                    }
                }
                return new StringWithValueFilter(input, filter);
            }
        }

    }

}

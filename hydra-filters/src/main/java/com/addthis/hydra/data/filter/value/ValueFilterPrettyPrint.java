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

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    private static final Pattern DOUBLE_QUOTE_PATTERN = Pattern.compile("\"");

    private static final String DOUBLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("\\\"");

    public static enum Format { JSON }

    /**
     * Formatting for output. Current options
     * are ["json"]. Default is "json".
     */
    @Nonnull private final Format format;

    @JsonCreator public ValueFilterPrettyPrint(@JsonProperty("format") Format format) {
        this.format = format;
    }

    @Override
    public ValueObject filter(ValueObject v) {
        return ValueFactory.create(prettyPrint(v, format));
    }

    @Override
    public ValueObject filterValue(ValueObject v) {
        return ValueFactory.create(prettyPrint(v, format));
    }

    public static String prettyPrint(ValueObject input, @Nonnull Format format) {
        if (input == null) {
            return null;
        }
        switch (format) {
            case JSON:
                return prettyPrintAsJson(input);
            default:
                throw new IllegalArgumentException("unknown format " + format);
        }
    }

    public static String prettyPrintAsJson(@Nonnull ValueObject input) {
        ValueObject.TYPE type = input.getObjectType();
        switch (type) {
            case INT:
            case FLOAT:
                return input.toString();
            case STRING:
            case CUSTOM:
                return prettyPrintString(input.toString());
            case BYTES: {
                byte[] bytes = input.asBytes().asNative();
                StringBuilder builder = new StringBuilder();
                builder.append("[");
                for (int i = 0; i < bytes.length; i++) {
                    builder.append(bytes[i]);
                    if (i < (bytes.length - 1)) {
                        builder.append(",");
                    }
                }
                builder.append("]");
                return builder.toString();
            }
            case ARRAY: {
                ValueArray array = input.asArray();
                StringBuilder builder = new StringBuilder();
                builder.append("[");
                Iterator<ValueObject> iterator = array.iterator();
                while (iterator.hasNext()) {
                    builder.append(prettyPrintAsJson(iterator.next()));
                    if (iterator.hasNext()) {
                        builder.append(",");
                    }
                }
                builder.append("]");
                return builder.toString();
            }
            case MAP: {
                ValueMap map = input.asMap();
                StringBuilder builder = new StringBuilder();
                builder.append("{");
                Iterator<Map.Entry<String,ValueObject>> iterator = map.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String,ValueObject> entry = iterator.next();
                    builder.append(prettyPrintString(entry.getKey()));
                    builder.append(":");
                    builder.append(prettyPrintAsJson(entry.getValue()));
                    if (iterator.hasNext()) {
                        builder.append(",");
                    }
                }
                builder.append("}");
                return builder.toString();
            }
            default:
                throw new IllegalStateException("Unrecognized object type " + type);
        }
    }

    public static String prettyPrintString(@Nonnull String input) {
        return "\"" + DOUBLE_QUOTE_PATTERN.matcher(input).replaceAll(DOUBLE_QUOTE_REPLACEMENT) + "\"";
    }

}

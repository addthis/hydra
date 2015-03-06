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

package com.addthis.hydra.query.web;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.QueryException;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;

public class DelimitedBundleEncoder extends AbstractBufferingHttpBundleEncoder {

    private final String delimiter;

    private final String quoteReplacement;

    private final static Pattern QUOTE_PATTERN = Pattern.compile("\"");

    public final static String SINGLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("'");

    public final static String ESCAPED_QUOTE_REPLACEMENT = Matcher.quoteReplacement("\\\"");

    DelimitedBundleEncoder(String filename, String delimiter, String quoteReplacement) {
        super();
        this.delimiter = delimiter;
        this.quoteReplacement = quoteReplacement;
        setContentTypeHeader(responseStart, "application/csv; charset=utf-8");
        responseStart.headers().set("Access-Control-Allow-Origin", "*");
        responseStart.headers().set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    public static DelimitedBundleEncoder create(String filename, String format) {
        String delimiter;
        String quoteReplacement;
        switch (format) {
            case "tsv":
                delimiter = "\t";
                quoteReplacement = SINGLE_QUOTE_REPLACEMENT;
                break;
            case "csv":
                delimiter = ",";
                quoteReplacement = SINGLE_QUOTE_REPLACEMENT;
                break;
            case "csv2":
                delimiter = ",";
                quoteReplacement = ESCAPED_QUOTE_REPLACEMENT;
                break;
            case "psv":
                delimiter = "|";
                quoteReplacement = SINGLE_QUOTE_REPLACEMENT;
                break;
            default:
                throw new QueryException("Invalid format \"" + format + "\"");
        }
        String suffix = format.substring(0, 3);
        if (!filename.toLowerCase().endsWith("." + suffix)) {
            filename = filename.concat("." + suffix);
        }
        return new DelimitedBundleEncoder(filename, delimiter, quoteReplacement);
    }

    public static String buildRow(Bundle row, String delimiter, String quoteReplacement) {
        StringBuilder stringBuilder = new StringBuilder(row.getFormat().getFieldCount() * 12 + 1);
        buildRow(row, delimiter, quoteReplacement, stringBuilder);
        return stringBuilder.toString();
    }

    private static String quoteString(String input, String quoteReplacement) {
        input = input.replace('\n', ' ').replace('\r', ' ');
        return QUOTE_PATTERN.matcher(input).replaceAll(quoteReplacement);
    }

    public static void buildRow(Bundle row, String delimiter, String quoteReplacement, StringBuilder stringBuilder) {
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                stringBuilder.append(delimiter);
            }
            if (o != null) {
                ValueObject.TYPE type = o.getObjectType();
                if (type == ValueObject.TYPE.CUSTOM) {
                    o = o.asCustom().asSimple();
                    type = o.getObjectType();
                }
                switch (type) {
                    case INT:
                    case FLOAT:
                        stringBuilder.append(o.toString());
                        break;
                    case STRING:
                        stringBuilder.append("\"");
                        stringBuilder.append(quoteString(o.toString(), quoteReplacement));
                        stringBuilder.append("\"");
                        break;
                    default:
                        break;
                }
            }
        }
        stringBuilder.append("\n");
    }

    @Override
    public void appendBundleToString(Bundle row, StringBuilder stringBuilder) {
        buildRow(row, delimiter, quoteReplacement, stringBuilder);
    }
}

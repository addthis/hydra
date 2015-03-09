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

import com.google.common.annotations.VisibleForTesting;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;

public class DelimitedEscapedBundleEncoder extends AbstractBufferingHttpBundleEncoder {

    private final String delimiter;

    private final static Pattern ESCAPE_CHARACTERS = Pattern.compile("([\\\\\\\"])");

    DelimitedEscapedBundleEncoder(String filename, String delimiter) {
        super();
        this.delimiter = delimiter;
        setContentTypeHeader(responseStart, "application/csv; charset=utf-8");
        responseStart.headers().set("Access-Control-Allow-Origin", "*");
        responseStart.headers().set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    public static DelimitedBundleEncoder create(String filename, String format) {
        String delimiter;
        switch (format) {
            case "csv2":
                delimiter = ",";
                break;
            case "psv2":
                delimiter = "|";
                break;
            case "tsv2":
                delimiter = "\t";
                break;
            default:
                throw new QueryException("Invalid format \"" + format + "\"");
        }
        String suffix = format.substring(0, 3);
        if (!filename.toLowerCase().endsWith("." + suffix)) {
            filename = filename.concat("." + suffix);
        }
        return new DelimitedBundleEncoder(filename, delimiter);
    }

    public static String buildRow(Bundle row, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder(row.getFormat().getFieldCount() * 12 + 1);
        buildRow(row, delimiter, stringBuilder);
        return stringBuilder.toString();
    }

    private static String quoteString(String input) {
        input = input.replace('\n', ' ').replace('\r', ' ');
        Matcher matcher = ESCAPE_CHARACTERS.matcher(input);
        return matcher.replaceAll("\\\\$1");
    }

    public static void buildRow(Bundle row, String delimiter, StringBuilder stringBuilder) {
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                stringBuilder.append(delimiter);
            }
            if (o != null) {
                buildValue(stringBuilder, o);
            }
        }
        stringBuilder.append("\n");
    }

    @VisibleForTesting
    static void buildValue(StringBuilder builder, ValueObject value) {
        ValueObject.TYPE type = value.getObjectType();
        if (type == ValueObject.TYPE.CUSTOM) {
            value = value.asCustom().asSimple();
            type = value.getObjectType();
        }
        switch (type) {
            case INT:
            case FLOAT:
                builder.append(value.toString());
                break;
            case STRING:
                builder.append("\"");
                builder.append(quoteString(value.toString()));
                builder.append("\"");
                break;
            default:
                break;
        }
    }

    @Override
    public void appendBundleToString(Bundle row, StringBuilder stringBuilder) {
        buildRow(row, delimiter, stringBuilder);
    }
}

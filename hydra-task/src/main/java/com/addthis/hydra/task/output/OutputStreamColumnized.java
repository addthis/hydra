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
package com.addthis.hydra.task.output;

import java.io.IOException;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.filter.value.StringFilter;

/**
 * Token Separated Column Values
 *
 * @user-reference
 * @hydra-name column
 */
public class OutputStreamColumnized extends OutputStreamFormatter implements SuperCodable {

    @FieldConfig(codable = true, required = true)
    private String[] columns;
    @FieldConfig(codable = true)
    private String stringQuote;
    @FieldConfig(codable = true)
    private String separator = "\t";
    @FieldConfig(codable = true)
    private String eol = "\n";
    @FieldConfig(codable = true)
    private StringFilter filter;
    @FieldConfig(codable = true)
    private String nameSeparator;

    private byte[] sepB;
    private byte[] stqB;
    private byte[] eolB;

    @Override
    public void open() {
        if (filter != null) {
            filter.open();
        }
    }

    @Override
    public OutputStreamEmitter createEmitter() {
        return new TokenOut();
    }

    private class TokenOut extends OutputStreamEmitter {

        private BundleFormat format;
        private BundleField[] fields;

        @Override
        public void write(OutputStream out, Bundle row) throws IOException {
            BundleFormat rowFormat = row.getFormat();
            if (fields == null || rowFormat != format) {
                BundleField[] newFields = new BundleField[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    newFields[i] = rowFormat.getField(columns[i]);
                }
                fields = newFields;
                format = rowFormat;
            }
            int rowlen = fields.length;
            for (int i = 0; i < rowlen; i++) {
                String val = ValueUtil.asNativeString(row.getValue(fields[i]));
                if (filter != null) {
                    val = filter.filter(val);
                }
                if (!Strings.isEmpty(val)) {
                    if (nameSeparator != null) {
                        val = fields[i].getName() + nameSeparator + val;
                    }
                    if (stringQuote != null && val.indexOf(separator) >= 0) {
                        out.write(stqB);
                        out.write(Bytes.toBytes(val));
                        out.write(stqB);
                    } else {
                        out.write(Bytes.toBytes(val));
                    }
                }
                if (i < rowlen - 1) {
                    out.write(sepB);
                }
            }
            out.write(eolB);
        }

        @Override
        public void flush(OutputStream out) throws IOException {
            out.flush();
        }
    }

    @Override
    public void postDecode() {
        sepB = separator != null ? Bytes.toBytes(separator) : null;
        stqB = stringQuote != null ? Bytes.toBytes(stringQuote) : null;
        eolB = Bytes.toBytes(eol);
    }

    @Override
    public void preEncode() {
    }
}

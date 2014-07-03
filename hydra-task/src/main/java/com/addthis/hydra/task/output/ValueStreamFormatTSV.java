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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;


public class ValueStreamFormatTSV extends ValuesStreamFormatter {

    private OutputStream out;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    @FieldConfig(codable = true)
    private String token = "\t";
    @FieldConfig(codable = true)
    private String replace = "_";
    @FieldConfig(codable = true)
    private String eol = "\n";
    @FieldConfig(codable = true)
    private boolean urlencode;

    byte[] byteToken;
    byte[] byteEOL;

    @Override
    public void init(OutputStream out) {
        this.out = out;
        byteToken = Bytes.toBytes(token);
        byteEOL = Bytes.toBytes(eol);
    }

    @Override
    public void output(Bundle row) {
        bos.reset();
        int off = 0;
        for (BundleField field : row) {
            ValueObject val = row.getValue(field);
            try {
                if (off++ > 0) {
                    bos.write(byteToken);
                }
                String str = val.toString();
                if (urlencode) {
                    str = Bytes.urlencode(str);
                }
                if (str.indexOf(token) >= 0) {
                    if (replace != null) {
                        str.replace(token, replace);
                    }
                }
                bos.write(Bytes.toBytes(str));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            bos.write(byteEOL);
            bos.writeTo(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
    }
}

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

import java.io.OutputStream;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.LessBytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;


public class ValueStreamFormatKV extends ValuesStreamFormatter {

    private OutputStream out;

    @FieldConfig(codable = true, required = true)
    private String[] columns;

    private BundleField[] fields;

    @Override
    public void flush() {
    }

    @Override
    public void init(OutputStream out) {
        this.out = out;
    }

    @Override
    public void output(Bundle row) {
        KVPairs kv = new KVPairs();
        if (fields == null) {
            fields = new BundleField[columns.length];
            for (int i = 0; i < columns.length; i++) {
                fields[i] = row.getFormat().getField(columns[i]);
            }
        }
        for (int i = 0; i < fields.length; i++) {
            ValueObject next = row.getValue(fields[i]);
            kv.add(fields[i].getName(), ValueUtil.asNativeString(next));
        }
        try {
            out.write(LessBytes.toBytes(kv.toString()));
            out.write('\n');
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}

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

import java.nio.charset.StandardCharsets;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

/**
 * @user-reference
 * @hydra-name json
 */
public class OutputStreamJson extends OutputStreamFormatter implements Codec.Codable {

    private static final byte[] newlineBytes = "\n".getBytes(StandardCharsets.UTF_8);

    @Override
    public OutputStreamEmitter createEmitter() {
        return new JsonOut();
    }

    private static class JsonOut extends OutputStreamEmitter {

        @Override
        public void write(OutputStream out, Bundle row) throws IOException {
            JSONObject jsonRow = new JSONObject();
            for (BundleField field : row) {
                ValueObject valueObject = row.getValue(field);
                String value;
                if (valueObject != null) {
                    value = valueObject.toString();
                } else {
                    value = "";
                }
                try {
                    jsonRow.put(field.getName(), value);
                } catch (JSONException ex) {
                    throw new RuntimeException(ex);
                }
            }
            out.write(jsonRow.toString().getBytes(StandardCharsets.UTF_8));
            out.write(newlineBytes);
        }

        @Override
        public void flush(OutputStream out) throws IOException {
            out.flush();
        }
    }
}

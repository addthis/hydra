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

import java.util.HashSet;

import java.nio.charset.StandardCharsets;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.maljson.JSONObject;

/**
 * @user-reference
 */
public class OutputStreamJson extends OutputStreamFormatter implements Codable {

    @FieldConfig(codable = true)
    private HashSet<String> include;
    @FieldConfig(codable = true)
    private HashSet<String> exclude;

    @Override
    public void open() { }

    private static final byte[] newlineBytes = "\n".getBytes(StandardCharsets.UTF_8);

    @Override
    public OutputStreamEmitter createEmitter() {
        return new JsonOut();
    }

    private class JsonOut extends OutputStreamEmitter {

        @Override
        public void write(OutputStream out, Bundle row) throws IOException {
            row = new FilteredBundle(row, include, exclude);
            JSONObject jsonRow = Bundles.toJSONObject(row);
            out.write(jsonRow.toString().getBytes(StandardCharsets.UTF_8));
            out.write(newlineBytes);
        }

        @Override
        public void flush(OutputStream out) throws IOException {
            out.flush();
        }
    }
}

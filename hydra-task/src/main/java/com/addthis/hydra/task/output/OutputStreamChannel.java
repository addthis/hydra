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

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.codec.Codec.Set;

/**
 * @user-reference
 * @hydra-name channel
 */
public class OutputStreamChannel extends OutputStreamFormatter {

    @Set(codable = true)
    private HashSet<String> include;
    @Set(codable = true)
    private HashSet<String> exclude;

    @Override
    public OutputStreamEmitter createEmitter() {
        return new OutputStreamEmitter() {
            private final ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
            private final FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();

            @Override
            public void write(OutputStream out, Bundle row) throws IOException {
                if (include != null || exclude != null) {
                    row = new FilteredBundle(row, include, exclude);
                }
                synchronized (this) {
                    Bytes.writeBytes(DataChannelCodec.encodeBundle(row, fieldMap, classMap), out);
                }
            }

            @Override
            public void flush(OutputStream out) throws IOException {
                out.flush();
            }
        };
    }
}

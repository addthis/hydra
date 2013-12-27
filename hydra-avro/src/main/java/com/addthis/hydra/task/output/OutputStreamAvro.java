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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

/**
 * @user-reference
 * @hydra-name avro
 */
public class OutputStreamAvro extends OutputStreamFormatter implements Codec.SuperCodable {

    @Set(codable = true)
    private HashSet<String> include;
    @Set(codable = true)
    private HashSet<String> exclude;
    @Set(codable = true, required = true)
    private String schema;
    // TODO: add support for specifying schema URL

    private Schema outputSchema;
    private DatumWriter<GenericRecord> datumWriter;


    @Override
    public OutputStreamEmitter createEmitter() {
        return new OutputStreamEmitter() {
            private final ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
            private final FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();
            private BinaryEncoder encoder;

            @Override
            public void write(OutputStream out, Bundle row) throws IOException {
                if (include != null || exclude != null) {
                    row = new FilteredBundle(row, include, exclude);
                }
                synchronized (this) {
                    GenericRecord outputRecord = createAvroRecordFromBundle(row);
                    encoder = EncoderFactory.get().blockingBinaryEncoder(out, encoder);
                    datumWriter.write(outputRecord, encoder);
                    Bytes.writeBytes(DataChannelCodec.encodeBundle(row, fieldMap, classMap), out);
                }
            }

            private GenericRecord createAvroRecordFromBundle(Bundle bundle) {
                GenericRecord genericRecord = new GenericData.Record(outputSchema);
                for (BundleField bundleField : bundle) {
                    ValueObject value = bundle.getValue(bundleField);
                    if (value == null) {
                        continue;
                    }
                    Object val = null;
                    switch (value.getObjectType()) {
                        case ARRAY:
                            ValueArray valueArray = value.asArray();
                            List<String> list = new ArrayList<>(valueArray.size());
                            for (ValueObject valueObject : valueArray) {
                                list.add(valueObject.toString());
                            }
                            val = list;
                            break;
                        case MAP:
                            ValueMap map = value.asMap();
                            Map<String, String> avroMap = new HashMap<>();
                            for (ValueMapEntry valueMapEntry : map) {
                                avroMap.put(valueMapEntry.getKey(), valueMapEntry.getValue().toString());
                            }
                            val = avroMap;
                            break;
                        case STRING:
                            val = value.asString().getString();
                            break;
                        case INT:
                            val = value.asNumber().asLong().getLong();
                            break;
                        case FLOAT:
                            val = value.asDouble().getDouble();
                            break;
                        case BYTES:
                            val = value.asBytes().getBytes();
                            break;
                    }

                    genericRecord.put(bundleField.getName(), val);
                }
                return genericRecord;
            }

            @Override
            public void flush(OutputStream out) throws IOException {
                out.flush();
            }
        };
    }

    @Override
    public void postDecode() {
        outputSchema = new Schema.Parser().parse(schema);
        datumWriter = new GenericDatumWriter<>(outputSchema);
    }

    @Override
    public void preEncode() {
    }
}

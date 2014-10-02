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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;

import com.google.common.annotations.Beta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

@Beta
public class OutputStreamAvro extends OutputStreamFormatter {

    private final Schema                     outputSchema;
    private final DatumWriter<GenericRecord> datumWriter;

    public OutputStreamAvro(String schema) {
        outputSchema = new Schema.Parser().parse(schema);
        datumWriter = new GenericDatumWriter<>(outputSchema);
    }

    @JsonCreator
    public OutputStreamAvro(JsonNode nodeSchema) {
        if (nodeSchema.hasNonNull("_optional-strings")) {
            ArrayNode fields = (ArrayNode) nodeSchema.get("fields");
            ArrayNode optionalStrings = (ArrayNode) nodeSchema.get("_optional-strings");
            Iterator<JsonNode> optionalStringIterator = optionalStrings.elements();
            while (optionalStringIterator.hasNext()) {
                String optionalString = optionalStringIterator.next().asText();
                ObjectNode wrapper = ((ObjectNode) nodeSchema).objectNode();
                ArrayNode unionType = wrapper.arrayNode();
                unionType.add("null");
                unionType.add("string");
                wrapper.put("name", optionalString);
                wrapper.set("type", unionType);
                fields.add(wrapper);
            }
        }
        String schema = nodeSchema.toString();
        outputSchema = new Schema.Parser().parse(schema);
        datumWriter = new GenericDatumWriter<>(outputSchema);
    }

    @Override
    public OutputStreamEmitter createEmitter() {
        return new OutputStreamEmitter() {
            private BinaryEncoder encoder;

            @Override
            public void write(OutputStream out, Bundle row) throws IOException {
                GenericRecord outputRecord = new GenericData.Record(outputSchema);
                populateAvroRecord(outputRecord, row);
                encoder = EncoderFactory.get().blockingBinaryEncoder(out, encoder);
                datumWriter.write(outputRecord, encoder);
                encoder.flush();
            }

            @Override
            public void flush(OutputStream out) throws IOException {
                out.flush();
            }
        };
    }

    public static void populateAvroRecord(GenericRecord genericRecord, Bundle bundle) {
        for (BundleField bundleField : bundle) {
            Schema.Field field = genericRecord.getSchema().getField(bundleField.getName());
            if (field == null) {
                continue;
            }
            ValueObject value = bundle.getValue(bundleField);
            if (value == null) {
                continue;
            }
            Object val = getAvroNativeFromValue(value, field.schema());
            genericRecord.put(bundleField.getName(), val);
        }
    }

    public static Object getAvroNativeFromValue(ValueObject value, Schema schema) {
        if ((value instanceof ValueString) && value.toString().isEmpty()) {
            return null;
        }
        if (schema.getType() == Schema.Type.UNION) {
            // wheel. of. fortune!
            for (Schema schemaOption : schema.getTypes()) {
                if (schemaOption.getType() != Schema.Type.NULL) {
                    schema = schemaOption;
                    break;
                }
            }
        }
        switch (schema.getType()) {
            case ARRAY:
                ValueArray valueArray = value.asArray();
                List<Object> list = new ArrayList<>(valueArray.size());
                for (ValueObject valueObject : valueArray) {
                    list.add(getAvroNativeFromValue(valueObject, schema.getElementType()));
                }
                return list;
            case MAP:
                ValueMap map = value.asMap();
                Map<String, Object> avroMap = new HashMap<>(value.asMap().size());
                for (ValueMapEntry valueMapEntry : map) {
                    avroMap.put(valueMapEntry.getKey(), getAvroNativeFromValue(valueMapEntry.getValue(),
                                                                               schema.getValueType()));
                }
                return avroMap;
            case STRING:
                return value.asString().asNative();
            case BYTES:
                return value.asBytes().asNative();
            case INT:
                return value.asLong().asNative().intValue();
            case LONG:
                return value.asLong().getLong();
            case FLOAT:
                return value.asDouble().asNative().floatValue();
            case DOUBLE:
                return value.asDouble().getDouble();
            case BOOLEAN:
                return Boolean.valueOf(value.toString());
            case NULL:
                return null;
            case RECORD:
                // todo: treat ValueMaps like bundles, but for now... hope for the best
            case ENUM:
                // hope for the best
            case UNION:
                // hope for the best
            case FIXED:
                // hope for the best
        }
        return value.asNative();
    }
}

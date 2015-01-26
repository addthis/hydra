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
package com.addthis.hydra.task.source;

import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.task.source.bundleizer.Bundleizer;
import com.addthis.hydra.task.source.bundleizer.BundleizerFactory;

import com.google.common.annotations.Beta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Beta
public class DataSourceAvro extends BundleizerFactory {
    private static final Logger log = LoggerFactory.getLogger(DataSourceAvro.class);

    private final Schema inputSchema;
    private final GenericDatumReader<GenericRecord> datumReader;

    public DataSourceAvro(String schema) {
        this.inputSchema = new Schema.Parser().parse(schema);
        this.datumReader = new GenericDatumReader<>(inputSchema);
    }

    @JsonCreator
    public DataSourceAvro(JsonNode nodeSchema) {
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
        inputSchema = new Schema.Parser().parse(schema);
        datumReader = new GenericDatumReader<>(inputSchema);
    }

    @Override
    public void open() { }

    public static ValueObject getValueObject(GenericRecord genericRecord,
                                                Schema.Field field,
                                                GenericData genericData) throws IOException {
        Object recordValue = genericRecord.get(field.name());
        if (recordValue == null) {
            // for some reason this is popular for some bundles..?
            return ValueFactory.create("");
        }
        return getValueObject(recordValue, field.schema(), genericData);
    }

    public static ValueObject getValueObject(Object recordValue,
                                                Schema schema,
                                                GenericData genericData) throws IOException {
        switch (schema.getType()) {
            case ARRAY:
                List<Object> recordArray = (List<Object>) recordValue;
                ValueArray replacement = ValueFactory.createArray(recordArray.size());
                for (Object arrayValue : recordArray) {
                    replacement.add(getValueObject(arrayValue,
                                                   schema.getElementType(),
                                                   genericData));
                }
                return replacement;
            case BYTES:
                return ValueFactory.create((byte[]) recordValue);
            case ENUM:
                return ValueFactory.create((double) recordValue);
            case FIXED:
                throw new RuntimeException("FIXED type is not supported");
            case FLOAT:
                // fall through
            case DOUBLE:
                return ValueFactory.create((double) recordValue);
            case INT:
                return ValueFactory.create((int) recordValue);
            case LONG:
                return ValueFactory.create((long) recordValue);
            case MAP:
                Map<String, Object> recordMap = (Map<String, Object>) recordValue;
                ValueMap newMap = ValueFactory.createMap();
                for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
                    String key = entry.getKey();
                    Object mapValue = entry.getValue();
                    ValueObject newValue = getValueObject(mapValue,
                                                             schema.getValueType(),
                                                             genericData);
                    newMap.put(key, newValue);
                }
                return newMap;
            case NULL:
                return null;
            case STRING:
                // fall through
            case BOOLEAN:
                return ValueFactory.create(recordValue.toString());
            case UNION:
                Schema unionSchema = schema.getTypes().get(genericData.resolveUnion(schema, recordValue));
                return getValueObject(recordValue, unionSchema, genericData);
            default:
                throw new IOException("Unknown schema type: " + schema);
        }
    }

    @Override public Bundleizer createBundleizer(final InputStream input,
                                                 final BundleFactory factory) {
        return new Bundleizer() {
            private final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(input, null);
            private GenericRecord reusableRecord = null;

            @Override public Bundle next() throws IOException {
                if (decoder.isEnd()) {
                    return null;
                }
                reusableRecord = datumReader.read(reusableRecord, decoder);
                GenericData genericData = datumReader.getData();
                Bundle bundle = factory.createBundle();
                for (Schema.Field field : inputSchema.getFields()) {
                    ValueObject value = DataSourceAvro.getValueObject(
                            reusableRecord, field, genericData);
                    if (value != null) {
                        bundle.setValue(bundle.getFormat().getField(field.name()), value);
                    }
                }
                return bundle;
            }
        };
    }
}

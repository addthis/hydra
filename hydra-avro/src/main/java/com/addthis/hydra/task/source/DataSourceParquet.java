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

import java.io.Closeable;
import java.io.IOException;

import java.util.List;
import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import com.google.common.annotations.Beta;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.avro.AvroParquetReader;

@Beta
public class DataSourceParquet implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DataSourceParquet.class);

    private final AvroParquetReader<GenericRecord> parquetReader;
    private final BundleFormat factory = new ListBundleFormat();

    public DataSourceParquet(String path) throws IOException {
        parquetReader = new AvroParquetReader<>(new Path(path));
    }

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

    public Bundle read() throws IOException {
        GenericRecord nextRecord = parquetReader.read();
        if (nextRecord == null) {
            return null;
        }
        GenericData genericData = GenericData.get();
        Bundle bundle = factory.createBundle();
        Schema inputSchema = nextRecord.getSchema();
        for (Schema.Field field : inputSchema.getFields()) {
            ValueObject value = DataSourceAvro.getValueObject(
                    nextRecord, field, genericData);
            if (value != null) {
                bundle.setValue(bundle.getFormat().getField(field.name()), value);
            }
        }
        return bundle;
    }

    @Override public void close() throws IOException {
        parquetReader.close();
    }
}

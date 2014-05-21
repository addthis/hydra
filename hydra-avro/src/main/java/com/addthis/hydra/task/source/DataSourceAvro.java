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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.task.run.TaskRunConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

/**
 * This data source <span class="hydra-summary">accepts avro streams</span>.
 *
 * @user-reference
 * @hydra-name avro
 */
public class DataSourceAvro extends TaskDataSource implements BundleFactory {

    /**
     * This field is required.
     */
    @Codec.Set(codable = true, required = true)
    protected FactoryInputStream input;
    @Codec.Set(codable = true, required = true)
    private String schema;
    @Codec.Set(codable = true, required = true)
    private HashSet<String> fields;

    private GenericDatumReader<GenericRecord> datumReader;

    private final ListBundleFormat format = new ListBundleFormat();
    private GenericRecord peekRecord;
    private Bundle peek;
    private Decoder decoder;
    private InputStream inputStream;

    @Override
    protected void open(TaskRunConfig config, AtomicBoolean errored) {

        setDatumReader(new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(schema)));
        try {
            setInputStream(input.createInputStream(config));
            setDecoder(DecoderFactory.get().binaryDecoder(inputStream, null));
        } catch (IOException e) {
            throw DataChannelError.promote(e);
        }
    }

    @Override
    public Bundle next() throws DataChannelError {
        Bundle next = peek();
        peek = null;
        return next;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        if (peek == null) {
            try {
                peekRecord = datumReader.read(peekRecord, decoder);
                GenericData genericData = datumReader.getData();
                Bundle bundle = createBundle();
                for (String field : fields) {
                    ValueObject value = getValueObject(peekRecord, field, genericData);
                    if (value != null) {
                        bundle.setValue(bundle.getFormat().getField(field), value);
                    }
                }
                peek = bundle;
            } catch (EOFException e) {
                return null;
            } catch (IOException e) {
                throw DataChannelError.promote(e);
            }
        }
        return peek;
    }

    protected ValueObject getValueObject(GenericRecord genericRecord, String fieldName, GenericData genericData) throws IOException {
        Schema.Field field = genericRecord.getSchema().getField(fieldName);
        if (field == null) {
            return null;
        }
        Object recordValue = genericRecord.get(field.name());
        if (recordValue == null) {
            return null;
        }
        Schema schema = field.schema();
        Schema.Type type = schema.getType();
        return getValueObject(recordValue, schema, type, genericData);
    }

    private ValueObject getValueObject(Object recordValue, Schema schema, Schema.Type type, GenericData genericData) throws IOException {
        ValueObject value = null;
        switch (type) {
        case ARRAY:
            List<String> replacement = new ArrayList<>();
            for (Utf8 av : (List<Utf8>) recordValue) {
                replacement.add(av.toString());
            }
            value = ValueFactory.createValueArray(replacement);
            break;
        case BYTES:
            value = ValueFactory.create((byte[]) recordValue);
            break;
        case ENUM:
            value = ValueFactory.create((double) recordValue);
            break;
        case FIXED:
            throw new RuntimeException("FIXED type is not supported");
        case FLOAT:
        case DOUBLE:
            value = ValueFactory.create((double) recordValue);
            break;
        case INT:
            value = ValueFactory.create((int) recordValue);
            break;
        case LONG:
            value = ValueFactory.create((long) recordValue);
            break;
        case MAP:
            throw new IOException("MAP types are not currently supported");
        case NULL:
            break;
        case STRING:
        case BOOLEAN:
            value = ValueFactory.create(recordValue.toString());
            break;
        case UNION:
            Schema unionSchema = schema.getTypes().get(genericData.resolveUnion(schema, recordValue));
            value = getValueObject(recordValue, unionSchema, unionSchema.getType(), genericData);
            break;
        default:
            throw new IOException("Unknown schema type: " + type);
        }
        return value;
    }

    @Override
    public void close() {
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public void setDatumReader(GenericDatumReader<GenericRecord> datumReader) {
        this.datumReader = datumReader;
    }

    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    public void setFields(HashSet<String> fields) {
        this.fields = fields;
    }
}

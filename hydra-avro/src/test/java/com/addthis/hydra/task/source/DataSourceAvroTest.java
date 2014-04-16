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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.Arrays;
import java.util.HashSet;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DataSourceAvroTest {

    private DataSourceAvro dataSourceAvro;
    private final String schema = "{\n  \"namespace\": \"com.addthis.camus.records\",\n  \"type\": \"record\",\n  \"name\": \"testEvent\",\n  \"doc\": \"test\",\n  \"fields\": [\n\t    " +
            "{\"name\": \"string\",\"type\": \"string\",\"default\": \"\"}," +
            "{\"name\": \"long\",\"type\": \"long\",\"default\": 0}," +
            "{\"name\": \"double\",\"type\": \"double\",\"default\": 0.0}," +
            "{\"name\": \"boolean\",\"type\": \"boolean\"}," +
            "{\"name\": \"array\", \"type\": {\"type\":\"array\", \"items\":\"string\"}}," +
            "{\"name\": \"union\", \"type\": [{\"type\":\"string\"}, {\"type\":\"null\"}]}" +
            "]}";

    @Before
    public void setup() {

        Schema outputSchema = new Schema.Parser().parse(schema);
        GenericRecord genericRecord = new GenericData.Record(outputSchema);
        genericRecord.put("string", "hello");
        genericRecord.put("long", 1l);
        genericRecord.put("double", 2.0d);
        genericRecord.put("boolean", true);
        genericRecord.put("array", Arrays.asList("foo", "bar", "car"));
        genericRecord.put("union", "union");

        dataSourceAvro = new DataSourceAvro();
        HashSet<String> fields = new HashSet<>();
        fields.add("string");
        fields.add("long");
        fields.add("double");
        fields.add("boolean");
        fields.add("array");
        fields.add("union");
        dataSourceAvro.setFields(fields);
        dataSourceAvro.setDatumReader(new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(schema)));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(outputSchema);
        try {
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStream inputStream = new ByteArrayInputStream(bos.toByteArray());
        dataSourceAvro.setInputStream(inputStream);
        dataSourceAvro.setDecoder(DecoderFactory.get().binaryDecoder(inputStream, null));

    }

    @Test
    public void testGetValueObject() throws Exception {

        Bundle bundle = dataSourceAvro.peek();
        assertNotNull(bundle);
        for (BundleField bundleField : bundle) {
            System.out.println(bundleField.getName() + ":" + bundle.getValue(bundleField));
        }
    }
}

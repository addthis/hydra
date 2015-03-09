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
package com.addthis.hydra.query.web;

import java.util.List;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.filter.value.ValueFilterJSON;
import com.addthis.hydra.data.filter.value.ValueFilterPrettyPrint;
import com.addthis.hydra.data.util.Tokenizer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DelimitedEscapedBundleEncoderTest {

    private static final ValueFilterJSON fromJSON = new ValueFilterJSON();
    private static final ValueFilterPrettyPrint toJSON = new ValueFilterPrettyPrint();

    @Test
    public void escapedDelimiters() {
        StringBuilder builder = new StringBuilder();
        DelimitedEscapedBundleEncoder.buildValue(builder, ValueFactory.create("abc\"def"));
        assertEquals("\"abc\\\"def\"", builder.toString());
        builder = new StringBuilder();
        DelimitedEscapedBundleEncoder.buildValue(builder, ValueFactory.create("abc\\def"));
        assertEquals("\"abc\\\\def\"", builder.toString());

    }

    private void csvEncodedJSONRoundTrip(ValueArray input) {
        ValueObject jsonString = toJSON.filter(input);
        StringBuilder builder = new StringBuilder();
        DelimitedEscapedBundleEncoder.buildValue(builder, jsonString);
        Tokenizer tokenizer = new Tokenizer().setGrouping(new String[]{"\""}).initialize();
        List<String> tokens = tokenizer.tokenize(builder.toString());
        assertEquals(1, tokens.size());
        ValueObject output = fromJSON.filterValue(ValueFactory.create(tokens.get(0)));
        assertEquals(input, output);
    }

    @Test
    public void arrayRoundTrip() throws Exception {
        ValueArray input = ValueFactory.createArray(3);
        input.add(ValueFactory.create("foo"));
        input.add(ValueFactory.create("bar"));
        input.add(ValueFactory.create("baz"));
        csvEncodedJSONRoundTrip(input);
    }

    @Test
    public void arrayWithQuotesRoundTrip() throws Exception {
        ValueArray input = ValueFactory.createArray(3);
        input.add(ValueFactory.create("foo"));
        input.add(ValueFactory.create("bar\"bar"));
        input.add(ValueFactory.create("baz"));
        csvEncodedJSONRoundTrip(input);
    }

    @Test
    public void arrayWithEscapeRoundTrip() throws Exception {
        ValueArray input = ValueFactory.createArray(3);
        input.add(ValueFactory.create("foo"));
        input.add(ValueFactory.create("bar\\bar"));
        input.add(ValueFactory.create("baz"));
        csvEncodedJSONRoundTrip(input);
    }

    @Test
    public void arrayWithSeveralEscapedRoundTrip() throws Exception {
        ValueArray input = ValueFactory.createArray(3);
        input.add(ValueFactory.create("foo\"hello\"world"));
        input.add(ValueFactory.create("bar\\bar\"baz\\\\quux"));
        input.add(ValueFactory.create("baz"));
        csvEncodedJSONRoundTrip(input);
    }
}

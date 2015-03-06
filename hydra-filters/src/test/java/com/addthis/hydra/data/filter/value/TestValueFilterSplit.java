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
package com.addthis.hydra.data.filter.value;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import org.junit.Test;

public class TestValueFilterSplit {

    private ValueObject splitFilter(ValueObject val, String split, int fixedLength) throws IOException {
        ValueFilterSplit vf = Configs.decodeObject(ValueFilterSplit.class,
                                                    "split: \"" + split + "\", fixedLength: " + fixedLength);
        return vf.filter(val);
    }

    @Test
    public void tokenize() throws IOException {
        ValueFilterSplit vf = Configs.decodeObject(ValueFilterSplit.class, "tokenizer: {group: [\"\\\"\"]} ");
        assertEquals(getValueArray("foo", "bar"), vf.filter(ValueFactory.create("foo,bar")));
        assertEquals(getValueArray("foo,bar", "baz"), vf.filter(ValueFactory.create("\"foo,bar\",baz")));
        assertEquals(getValueArray("foo,", "bar"), vf.filter(ValueFactory.create("foo\\,,bar")));
        assertEquals(null, vf.filter(ValueFactory.create("")));
    }

    @Test
    public void nullPassThrough() throws IOException {
        assertEquals(null, splitFilter(null, ",", 0));
    }

    @Test
    public void changeCase() throws IOException {
        ValueArray t1 = getValueArray("foo", "bar");
        assertEquals(t1, splitFilter(new ValueFilterJoin().filter(t1), ",", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foo,bar"), ",", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foo | bar"), " | ", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foobar"), null, 3));
    }
    
    @Test
    public void splitArray()  throws IOException {
        ValueArray t1 = getValueArray("foo", "bar");
        assertEquals(t1, splitFilter(t1, ",", -1));
    }

    private ValueArray getValueArray(String... values) {
        ValueArray t1 = ValueFactory.createArray(2);
        for (String value : values) {
            t1.add(ValueFactory.create(value));
        }
        return t1;
    }

}

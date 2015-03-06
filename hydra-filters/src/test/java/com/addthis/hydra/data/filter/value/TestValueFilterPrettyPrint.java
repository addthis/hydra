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

import java.util.Set;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestValueFilterPrettyPrint {

    private void test(ValueFilterPrettyPrint vf, ValueObject input, String output) {
        ValueObject result1 = vf.filter(input);
        ValueObject result2 = vf.filterValue(input);
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(ValueObject.TYPE.STRING, result1.getObjectType());
        assertEquals(ValueObject.TYPE.STRING, result2.getObjectType());
        assertEquals(output, result1.toString());
        assertEquals(output, result2.toString());
    }

    private void test(ValueFilterPrettyPrint vf, ValueObject input, Set<String> output) {
        ValueObject result1 = vf.filter(input);
        ValueObject result2 = vf.filterValue(input);
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(ValueObject.TYPE.STRING, result1.getObjectType());
        assertEquals(ValueObject.TYPE.STRING, result2.getObjectType());
        assertTrue("result is " + result1.toString(), output.contains(result1.toString()));
        assertTrue("result is " + result2.toString(), output.contains(result2.toString()));
    }

    @Test
    public void printNull() throws IOException {
        ValueFilterPrettyPrint vf = Configs.decodeObject(ValueFilterPrettyPrint.class, "");
        assertEquals(null, vf.filter(null));
        assertEquals(null, vf.filterValue(null));
    }

    @Test
    public void printString() throws IOException {
        ValueFilterPrettyPrint vf = Configs.decodeObject(ValueFilterPrettyPrint.class, "");
        test(vf, ValueFactory.create(""), "\"\"");
        test(vf, ValueFactory.create("foo"), "\"foo\"");
        test(vf, ValueFactory.create("foo\"bar"), "\"foo\\\"bar\"");
        test(vf, ValueFactory.create("foo,bar"), "\"foo,bar\"");
    }

    @Test
    public void printArray() throws IOException {
        ValueFilterPrettyPrint vf = Configs.decodeObject(ValueFilterPrettyPrint.class, "");
        ValueArray array = ValueFactory.createArray(3);
        test(vf, array, "[]");
        array.add(ValueFactory.create(1));
        array.add(ValueFactory.create(2));
        array.add(ValueFactory.create(3));
        test(vf, array, "[1,2,3]");
        array.clear();
        array.add(ValueFactory.create("foo"));
        array.add(ValueFactory.create("bar"));
        array.add(ValueFactory.create("baz\"quux"));
        test(vf, array, "[\"foo\",\"bar\",\"baz\\\"quux\"]");
    }

    @Test
    public void printMap() throws IOException {
        ValueFilterPrettyPrint vf = Configs.decodeObject(ValueFilterPrettyPrint.class, "");
        ValueMap map = ValueFactory.createMap();
        test(vf, map, "{}");
        map.put("foo", ValueFactory.create(1));
        map.put("bar", ValueFactory.create("baz\"quux"));
        test(vf, map, ImmutableSet.of("{\"foo\":1,\"bar\":\"baz\\\"quux\"}",
                                      "{\"bar\":\"baz\\\"quux\",\"foo\":1}"));
    }

}

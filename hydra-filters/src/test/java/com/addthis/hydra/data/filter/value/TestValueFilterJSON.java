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

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterJSON {

    private static final ValueFilterJSON json = new ValueFilterJSON();
    private static final ValueFilterPrettyPrint print = new ValueFilterPrettyPrint();

    private void roundTrip(ValueObject input) {
        assertEquals(input, json.filterValue(print.filterValue(input)));
    }

    @Test
    public void parseNull() throws IOException {
        assertEquals(null, json.filterValue(null));
        roundTrip(null);
    }

    @Test
    public void parseString() throws IOException {
        assertEquals(ValueFactory.create(""), json.filterValue(ValueFactory.create("\"\"")));
        assertEquals(ValueFactory.create("foo"), json.filterValue(ValueFactory.create("\"foo\"")));
        assertEquals(ValueFactory.create("foo\"bar"), json.filterValue(ValueFactory.create("\"foo\\\"bar\"")));
        assertEquals(ValueFactory.create("foo,bar"), json.filterValue(ValueFactory.create("\"foo,bar\"")));
        roundTrip(ValueFactory.create(""));
        roundTrip(ValueFactory.create("foo"));
        roundTrip(ValueFactory.create("foo\"bar"));
        roundTrip(ValueFactory.create("foo,bar"));
    }

    @Test
    public void parseArray() throws IOException {
        ValueArray array = ValueFactory.createArray(3);
        assertEquals(array, json.filterValue(ValueFactory.create("[]")));
        roundTrip(array);
        array.add(ValueFactory.create(1));
        array.add(ValueFactory.create(2));
        array.add(ValueFactory.create(3));
        assertEquals(array, json.filterValue(ValueFactory.create("[1,2,3]")));
        roundTrip(array);
        array.clear();
        array.add(ValueFactory.create("foo"));
        array.add(ValueFactory.create("bar"));
        array.add(ValueFactory.create("baz\"quux"));
        assertEquals(array, json.filterValue(ValueFactory.create("[\"foo\",\"bar\",\"baz\\\"quux\"]")));
        roundTrip(array);
    }

    @Test
    public void parseMap() throws IOException {
        ValueMap map = ValueFactory.createMap();
        assertEquals(map, json.filterValue(ValueFactory.create("{}")));
        roundTrip(map);
        map.put("foo", ValueFactory.create(1));
        map.put("bar", ValueFactory.create("baz\"quux"));
        assertEquals(map, json.filterValue(ValueFactory.create("{\"foo\":1,\"bar\":\"baz\\\"quux\"}")));
        roundTrip(map);
    }

}

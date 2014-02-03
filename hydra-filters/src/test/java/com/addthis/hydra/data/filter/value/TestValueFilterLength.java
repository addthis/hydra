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

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterLength {

    @Test
    public void filterLength() {
        ValueFilterLength vfl = new ValueFilterLength();
        assertEquals(null, vfl.filter(null));
        assertEquals(ValueFactory.create(0), vfl.filter(ValueFactory.create("")));
        assertEquals(ValueFactory.create(3), vfl.filter(ValueFactory.create("abc")));
    }

    @Test
    public void filterLengthArray() {
        ValueFilterLength vfl = new ValueFilterLength();
        ValueArray array = ValueFactory.createArray(0);
        assertEquals(null, vfl.filter(ValueFactory.createArray(0)));
        // prevent auto-filter iteration
        vfl.setOnce(true);
        assertEquals(ValueFactory.create(0), vfl.filter(array));
        // add elements, try again
        array.add(ValueFactory.create("foo"));
        array.add(ValueFactory.create(5));
        // there should be two elements
        assertEquals(ValueFactory.create(2), vfl.filter(array));
        // disable once so each value is passed through the filter
        vfl.setOnce(false);
        array = (ValueArray) vfl.filter(array);
        assertEquals(ValueFactory.create(3), array.get(0)); // length of "foo"
        assertEquals(ValueFactory.create(5), array.get(1)); // length of the # 5
    }
}

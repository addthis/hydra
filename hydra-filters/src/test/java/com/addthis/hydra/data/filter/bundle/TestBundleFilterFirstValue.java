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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestBundleFilterFirstValue {

    @Test
    public void testBundleFilterFirst() {
        BundleFilterFirstValue bff = new BundleFilterFirstValue();
        bff.setIn(new String[]{"a", "b", "c", "d"});
        bff.setOut("out");

        MapBundle b1 = MapBundle.createBundle(new String[]{
                "a", "123",
                "b", "234",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("123", b1.get("out"));

        b1 = MapBundle.createBundle(new String[]{
                "a", "",
                "b", "234",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("234", b1.get("out"));

        b1 = MapBundle.createBundle(new String[]{
                "a", null,
                "b", "",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("345", b1.get("out"));

        ValueArray array = ValueFactory.createArray(0);
        b1.setValue(b1.getFormat().getField("b"), array);
        assertTrue(bff.filter(b1));
        assertEquals("345", b1.get("out"));

        array = ValueFactory.createArray(1);
        array.add(ValueFactory.create("foo"));
        b1.setValue(b1.getFormat().getField("b"), array);
        assertTrue(bff.filter(b1));
        // MapBundle converts all values to String, messing up the test
        assertTrue(!b1.get("out").equals("345"));
    }

    @Test
    public void testBundleFilterFirstWhich() {
        BundleFilterFirstValue bff = new BundleFilterFirstValue();
        bff.setIn(new String[]{"a", "b", "c", "d"});
        bff.setOut("out");
        bff.setWhich("whichField");

        MapBundle b1 = MapBundle.createBundle(new String[]{
                "a", "123",
                "b", "234",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("123", b1.get("out"));
        assertEquals("a", b1.get("whichField"));

        b1 = MapBundle.createBundle(new String[]{
                "a", "",
                "b", "234",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("234", b1.get("out"));
        assertEquals("b", b1.get("whichField"));

        b1 = MapBundle.createBundle(new String[]{
                "a", null,
                "b", "",
                "c", "345",
                "d", "456"
        });
        assertTrue(bff.filter(b1));
        assertEquals("345", b1.get("out"));
        assertEquals("c", b1.get("whichField"));

        ValueArray array = ValueFactory.createArray(0);
        b1.setValue(b1.getFormat().getField("b"), array);
        assertTrue(bff.filter(b1));
        assertEquals("345", b1.get("out"));
        assertEquals("c", b1.get("whichField"));

        array = ValueFactory.createArray(1);
        array.add(ValueFactory.create("foo"));
        b1.setValue(b1.getFormat().getField("b"), array);
        assertTrue(bff.filter(b1));
        // MapBundle converts all values to String, messing up the test
        assertTrue(!b1.get("out").equals("345"));
        assertEquals("b", b1.get("whichField"));
    }

}

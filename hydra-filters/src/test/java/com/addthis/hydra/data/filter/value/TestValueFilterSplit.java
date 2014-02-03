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

import java.lang.reflect.Field;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterSplit {

    private ValueObject splitFilter(ValueObject val, String split, int fixedLength) {
        ValueFilterSplit vfc = new ValueFilterSplit();
        try {
            Field splitField = vfc.getClass().getDeclaredField("split");
            splitField.setAccessible(true);
            splitField.set(vfc, split);

            Field fixedLengthField = vfc.getClass().getDeclaredField("fixedLength");
            fixedLengthField.setAccessible(true);
            fixedLengthField.set(vfc, fixedLength);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }

        return vfc.filter(val);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, splitFilter(null, ",", 0));
    }

    @Test
    public void changeCase() {
        ValueArray t1 = getValueArray("foo", "bar");
        assertEquals(t1, splitFilter(t1, ",", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foo,bar"), ",", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foo | bar"), " | ", -1));
        assertEquals(t1, splitFilter(ValueFactory.create("foobar"), null, 3));
    }

    private ValueArray getValueArray(String... values) {
        ValueArray t1 = ValueFactory.createArray(2);
        for (String value : values) {
            t1.add(ValueFactory.create(value));
        }
        return t1;
    }

}

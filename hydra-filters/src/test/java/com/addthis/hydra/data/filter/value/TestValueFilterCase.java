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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterCase {

    private ValueObject caseFilter(String val, boolean lower, boolean upper) {
        ValueFilterCase vfc = new ValueFilterCase();
        try {
            Field lowerField = vfc.getClass().getDeclaredField("lower");
            lowerField.setAccessible(true);
            lowerField.set(vfc, lower);

            Field upperField = vfc.getClass().getDeclaredField("upper");
            upperField.setAccessible(true);
            upperField.set(vfc, upper);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }

        return vfc.filter(ValueFactory.create(val));
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, caseFilter(null, true, true));
    }

    @Test
    public void changeCase() {
        assertEquals(ValueFactory.create("FOO"), caseFilter("foo", false, true));
        assertEquals(ValueFactory.create("foo"), caseFilter("FOO", true, false));
        assertEquals(ValueFactory.create("FOO"), caseFilter("foo", true, true));
        assertEquals(ValueFactory.create("FOO"), caseFilter("FOO", true, true));
    }

}

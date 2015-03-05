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

import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterSlice {

    @Test
    public void stringSlice() {
        ValueFilterStringSlice vf = new ValueFilterStringSlice(",", "|", 0, 2);
        assertEquals("a|b", vf.filter("a,b,c"));
        assertEquals(null, vf.filter((String) null));
    }

    @Test
    public void trickyStringSlice() {
        ValueFilterStringSlice vf = new ValueFilterStringSlice(",", 1);
        assertEquals("2,3,4,5", vf.filter("1,2,3,4,5"));
    }

    @Test
    public void slice() {
        ValueFilterSlice vf = new ValueFilterSlice(2, 4, 1);
        assertEquals("23", vf.filter(ValueFactory.create("0123456")).toString());
        vf = new ValueFilterSlice(2, -1, 1);
        assertEquals("23456", vf.filter(ValueFactory.create("0123456")).toString());
        vf = new ValueFilterSlice(2, -2, 1);
        assertEquals("2345", vf.filter(ValueFactory.create("0123456")).toString());
    }
}

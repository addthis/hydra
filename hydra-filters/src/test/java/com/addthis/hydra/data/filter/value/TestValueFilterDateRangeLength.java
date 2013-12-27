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

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestValueFilterDateRangeLength {

    @Test
    public void simpleDates() {
        ValueFilterDateRangeLength vfdrl = new ValueFilterDateRangeLength();
        assertEquals("11", vfdrl.filter("090910-090920"));
        assertEquals("42", vfdrl.filter("090810-090920"));
    }

    @Test
    public void singleDay() {
        ValueFilterDateRangeLength vfdrl = new ValueFilterDateRangeLength();
        assertEquals("1", vfdrl.filter("090910"));
    }

    @Test
    public void dayRanges() {
        ValueFilterDateRangeLength vfdrl = new ValueFilterDateRangeLength();
        assertEquals("2", vfdrl.filter("090910,101010"));
        assertEquals("12", vfdrl.filter("090910,101010-101020"));
    }

    @Test
    public void nullChecks() {
        ValueFilterDateRangeLength vfdrl = new ValueFilterDateRangeLength();
        assertEquals(null, vfdrl.filter((String) null));
        assertEquals(null, vfdrl.filter("090909090909-"));
        assertEquals(null, vfdrl.filter("090909090909"));
        assertEquals(null, vfdrl.filter("1090810-090920"));
    }

    @Test
    public void formatOptions() {
        ValueFilterDateRangeLength vfdrl = new ValueFilterDateRangeLength("yyyy-MM-dd", "+", "days");
        assertEquals("6", vfdrl.filter("2010-10-10+2010-10-15"));
    }

    // Not currently supported
    // @Test
    // public void diffUnitOptions() {
    // ValueFilterDateRangeLength vfdrl = new
    // ValueFilterDateRangeLength("yyyy-MM-dd", "!", "days");
    // assertEquals("1", vfdrl.filter("2010-10-10!2010-10-11"));
    // vfdrl = new ValueFilterDateRangeLength("yyyy-MM-dd", "!", "hours");
    // assertEquals("24", vfdrl.filter("2010-10-10!2010-10-11"));
    // vfdrl = new ValueFilterDateRangeLength("yyyy-MM-dd", "!", "minutes");
    // assertEquals("1440", vfdrl.filter("2010-10-10!2010-10-11"));
    // vfdrl = new ValueFilterDateRangeLength("yyyy-MM-dd", "!", "seconds");
    // assertEquals("86400", vfdrl.filter("2010-10-10!2010-10-11"));
    // vfdrl = new ValueFilterDateRangeLength("yyyy-MM-dd", "!", "ms");
    // assertEquals("86400000", vfdrl.filter("2010-10-10!2010-10-11"));
    // }

}

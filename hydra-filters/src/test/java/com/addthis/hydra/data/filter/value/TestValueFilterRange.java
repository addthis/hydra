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

public class TestValueFilterRange {

    private String rangeFilter(String val, int start, int end) {
        return new ValueFilterRange().setRange(start, end).filter(val);
    }

    @Test
    public void testNullPas() {
        assertEquals(null, rangeFilter(null, 0, -1));
    }

    @Test
    public void simpleStrings() {
        assertEquals("0", rangeFilter("0123456789", 0, 1));
        assertEquals("12345678", rangeFilter("0123456789", 1, -1));
        assertEquals("23456789", rangeFilter("0123456789", 2, 20));
    }
}

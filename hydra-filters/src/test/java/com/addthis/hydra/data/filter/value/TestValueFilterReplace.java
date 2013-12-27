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

public class TestValueFilterReplace {

    private String replaceFilter(String val, String find, String replace, boolean regex) {
        return new ValueFilterReplace().setFind(find).setReplace(replace).setRegex(regex).filter(val);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, replaceFilter(null, null, null, false));
    }

    @Test
    public void simpleReplace() {
        assertEquals("FooBax", replaceFilter("FooBarBax", "Bar", "", false));
        assertEquals("FooQuxBax", replaceFilter("FooBarBax", "Bar", "Qux", false));
    }

    @Test
    public void replaceCommaSpaces() {
        assertEquals("192.168.1.1,123.456.789.123,1.3.3.7 8 - [08/Oct/2012:00:00:06 -0400] ",
                replaceFilter("192.168.1.1    ,123.456.789.123 ,1.3.3.7 8 - [08/Oct/2012:00:00:06 -0400] ", "\\s+,", ",", true));
    }


    @Test
    public void regexReplace() {
        assertEquals("FooBax", replaceFilter("FooBarBax", "Bar", "", true));
        assertEquals("FooQuxBax", replaceFilter("FooBarBax", "Bar", "Qux", true));
        assertEquals("FooQuxBax", replaceFilter("Foo12Bax", "\\d\\d", "Qux", true));
    }

}

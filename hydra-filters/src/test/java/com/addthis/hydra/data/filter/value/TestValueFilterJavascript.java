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

import com.addthis.basis.test.SlowTest;

import com.addthis.codec.config.Configs;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
public class TestValueFilterJavascript {

    @Test
    public void mainPort() throws Exception {
        ValueFilterJavascript f = Configs.decodeObject(ValueFilterJavascript.class, "source: \"value.length\"");

        String[] str = {"now is the time", "for all good men", "to come to", "the aid of the party"};

        assertEquals("15", f.filter(str[0]));
        assertEquals("16", f.filter(str[1]));
        assertEquals("10", f.filter(str[2]));
        assertEquals("20", f.filter(str[3]));
    }
}

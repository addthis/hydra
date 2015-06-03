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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterCache {

    @Test
    public void filter() throws IOException {
        ValueFilterCache filter = Configs.decodeObject(ValueFilterCache.class, "filter.case.upper: true");
        assertEquals(ValueFactory.create("FOO"), filter.filter(ValueFactory.create("foo")));
        assertEquals(ValueFactory.create("FOO"), filter.filter(ValueFactory.create("bar")));
        assertEquals(ValueFactory.create("FOO"), filter.filter(ValueFactory.create("baz")));
    }
}

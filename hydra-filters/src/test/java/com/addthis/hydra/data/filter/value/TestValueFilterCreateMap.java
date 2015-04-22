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

import java.util.Set;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestValueFilterCreateMap {

    @Test
    public void createMap() throws IOException {
        ValueFilterCreateMap vf = Configs.decodeObject(ValueFilterCreateMap.class, "");
        ValueMap result = vf.filter(ValueFactory.create("hello=world,foo=bar")).asMap();
        assertEquals("world", result.get("hello").asString().asNative());
        assertEquals("bar", result.get("foo").asString().asNative());
        result = vf.filter(ValueFactory.create("hello=world,hello=bar")).asMap();
        assertEquals("bar", result.get("hello").asString().asNative());
        result = vf.filter(ValueFactory.create("hello=world,hello")).asMap();
        assertEquals("world", result.get("hello").asString().asNative());
    }

    @Test
    public void append() throws IOException {
        ValueFilterCreateMap vf = Configs.decodeObject(ValueFilterCreateMap.class, "appendValues=true");
        ValueMap result = vf.filter(ValueFactory.create("hello=world,baz=quux,hello=bar,hello=world")).asMap();
        ValueArray values = result.get("hello").asArray();
        assertEquals(3, values.size());
        assertEquals("world", values.get(0).asString().asNative());
        assertEquals("bar", values.get(1).asString().asNative());
        assertEquals("world", values.get(2).asString().asNative());
        assertEquals("quux", result.get("baz").asString().asNative());
    }

}

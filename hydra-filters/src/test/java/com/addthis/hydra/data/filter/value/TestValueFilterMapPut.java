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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterMapPut {
    @Test
    public void setKey() throws Exception {
        ValueFilterMapPut filter = Configs.decodeObject(ValueFilterMapPut.class, "key:mykey,value:foobar");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        ValueMap<String> filtered = (ValueMap<String>) filter.filter(map);
        assertEquals("foobar", filtered.asMap().get("mykey").asNative());
    }

    @Test
    public void replaceKey() throws Exception {
        ValueFilterMapPut filter = Configs.decodeObject(ValueFilterMapPut.class, "key:mykey,value:foobar");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        map.put("mykey", ValueFactory.create("blah"));
        ValueMap<String> filtered = (ValueMap<String>) filter.filter(map);
        assertEquals("foobar", filtered.asMap().get("mykey").asNative());
    }
}

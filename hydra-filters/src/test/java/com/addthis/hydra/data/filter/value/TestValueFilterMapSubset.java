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
import com.addthis.bundle.value.ValueMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.codec.config.Configs.decodeObject;
import static com.addthis.codec.config.Configs.newDefault;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestValueFilterMapSubset {
    private static final Logger log = LoggerFactory.getLogger(TestValueFilterMapSubset.class);

    @Test public void nullPassThrough() throws Exception {
        ValueFilterMapSubset mapSubsetFilter = newDefault(ValueFilterMapSubset.class);
        assertNull(mapSubsetFilter.filter(null));
    }

    @Test public void toMap() throws Exception {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueFilterMapSubset mapSubsetFilter = newDefault(ValueFilterMapSubset.class);
        assertEquals(map, mapSubsetFilter.filter(map));
    }

    @Test public void whiteList() throws Exception {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1", "k2=v2");
        ValueFilterMapSubset mapSubsetFilter = decodeObject(ValueFilterMapSubset.class, "whitelist = [k1, k2]");

        assertEquals(result, mapSubsetFilter.filter(map));
    }

    @Test public void blackList() throws Exception {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1", "k2=v2");
        ValueFilterMapSubset mapSubsetFilter = decodeObject(ValueFilterMapSubset.class, "blacklist = k3");

        assertEquals(result, mapSubsetFilter.filter(map));
    }

    @Test public void whitelistAndBlacklist() throws Exception {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1");
        ValueFilterMapSubset mapSubsetFilter = decodeObject(ValueFilterMapSubset.class,
                                                            "whitelist = k1, blacklist = k3");
        assertEquals(result, mapSubsetFilter.filter(map));
    }

    @Test public void string() throws Exception {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueFilterMapSubset mapSubsetFilter = decodeObject(ValueFilterMapSubset.class, "toString = true");

        assertEquals("k1=v1,k2=v2,k3=v3", mapSubsetFilter.filter(map).toString());
    }

    private static ValueMap createMap(String... kvPairs) {
        ValueMap valueMap = ValueFactory.createMap();

        for (String kvPair : kvPairs) {
            String[] kv = kvPair.split("=");
            valueMap.put(kv[0], ValueFactory.create(kv[1]));
        }

        return valueMap;
    }
}

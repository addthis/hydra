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
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test the Value Filter for creating a subset of a map
 */
public class TestValueFilterMapSubset {

    private ValueObject subsetOfMap(ValueObject value, String[] whitelist, String[] blacklist,
            boolean toString, String keySep, String valueSep) {
        ValueFilterMapSubset mapSubset = new ValueFilterMapSubset();

        if (whitelist != null) {
            setField(mapSubset, "whitelist", whitelist);
        }

        if (blacklist != null) {
            setField(mapSubset, "blacklist", blacklist);
        }

        setField(mapSubset, "toString", toString);

        if (keySep != null) {
            setField(mapSubset, "keySep", keySep);
        }

        if (valueSep != null) {
            setField(mapSubset, "valueSep", valueSep);
        }

        return mapSubset.filter(value);
    }

    private void setField(ValueFilterMapSubset mapSubset, String fieldKey, Object value) {
        try {
            Field field = mapSubset.getClass().getDeclaredField(fieldKey);
            field.setAccessible(true);

            if (field != null) {
                field.set(mapSubset, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, subsetOfMap(null, null, null, false, null, null));
    }

    @Test
    public void toMap() {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        assertEquals(map, subsetOfMap(map, null, null, false, null, null));
    }

    @Test
    public void toMap_WhiteList() {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1", "k2=v2");

        assertEquals(result, subsetOfMap(map, new String[]{"k1", "k2"}, null, false, null, null));
    }

    @Test
    public void toMap_BlackList() {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1", "k2=v2");

        assertEquals(result, subsetOfMap(map, null, new String[]{"k3"}, false, null, null));
    }

    @Test
    public void toMap_Whitelist_BlackList() {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");
        ValueMap result = createMap("k1=v1");

        assertEquals(result, subsetOfMap(map, new String[]{"k1"}, new String[]{"k3"}, false, null, null));
    }

    @Test
    public void toMap_String() {
        ValueMap map = createMap("k1=v1", "k2=v2", "k3=v3");

        assertEquals("k1=v1,k2=v2,k3=v3", subsetOfMap(map, null, null, true, "=", ",").toString());
    }

    private ValueMap createMap(String... kvPairs) {
        ValueMap valueMap = ValueFactory.createMap();

        for (String kvPair : kvPairs) {
            String[] kv = kvPair.split("\\=");
            valueMap.put(kv[0], ValueFactory.create(kv[1]));
        }

        return valueMap;
    }

}

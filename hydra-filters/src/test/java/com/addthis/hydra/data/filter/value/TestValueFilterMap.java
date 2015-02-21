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

import java.util.HashMap;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static com.addthis.codec.config.Configs.decodeObject;
import static java.util.Optional.ofNullable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestValueFilterMap {

    private String mapFilter(String val, HashMap<String, String> map, String mapURL, boolean tonull) {
        ValueFilterMap filter = new ValueFilterMap().setMap(map).setMapURL(mapURL).setToNull(tonull);
        filter.postDecode();
        return ofNullable(filter.filter(ValueFactory.create(val))).map(Object::toString).orElse(null);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, mapFilter(null, new HashMap<String, String>(), null, false));
    }

    @Test public void contextualLookup() throws IOException {
        ValueFilter valueFilter = decodeObject(ValueFilter.class, "map { map.field = lookup }");
        Bundle bundle = Bundles.decode("lookup = {a = 1, b = 2, c = 3, d = 4}");
        ValueObject val = ValueFactory.decodeValue("c");
        assertEquals("3", valueFilter.filter(val, bundle).toString());
    }

    @Test
    public void simpleReplace() {
        HashMap<String, String> map = new HashMap<>();
        map.put("up", "down");
        map.put("charm", "strange");
        map.put("huh", null);
        assertEquals("foo", mapFilter("foo", map, null, false));
        assertEquals("down", mapFilter("up", map, null, false));
        assertEquals("down", mapFilter("up", map, null, true));
        assertEquals("huh", mapFilter("huh", map, null, false));
        assertEquals(null, mapFilter("huh", map, null, true));
    }

    @Test
    public void badURL() {
        testOneBadUrl("http://asdsadsdasdds/");
    }

    private void testOneBadUrl(String url) {
        boolean failure = false;
        try {
            mapFilter("foo", null, url , false);
        } catch (Exception ex) {
            failure = true;
        }
        assertTrue(failure);
    }

}

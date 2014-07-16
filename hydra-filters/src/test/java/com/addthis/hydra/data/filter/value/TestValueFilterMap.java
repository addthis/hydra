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

import java.util.HashMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterMap {

    private String mapFilter(String val, HashMap<String, String> map, boolean tonull) {
        return new ValueFilterMap().setMap(map).setToNull(tonull).filter(val);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, mapFilter(null, new HashMap<String, String>(), false));
    }

    @Test
    public void simpleReplace() {
        HashMap<String, String> map = new HashMap<>();
        map.put("up", "down");
        map.put("charm", "strange");
        map.put("huh", null);
        assertEquals("foo", mapFilter("foo", map, false));
        assertEquals("down", mapFilter("up", map, false));
        assertEquals("down", mapFilter("up", map, true));
        assertEquals("huh", mapFilter("huh", map, false));
        assertEquals(null, mapFilter("huh", map, true));
    }

}

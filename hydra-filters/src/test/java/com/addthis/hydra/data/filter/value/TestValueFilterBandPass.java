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

import java.util.HashSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterBandPass {

    // TODO: Need to pass multiple items to the same filter instnace
    // to really test this.
    private String bandPassFilter(String val, int minHits, int maxHits, int maxKeys, HashSet<String> whitelist) {
        ValueFilterBandPass vfbp = new ValueFilterBandPass();
        try {
            Field minHitsField = vfbp.getClass().getDeclaredField("minHits");
            minHitsField.setAccessible(true);
            minHitsField.set(vfbp, minHits);

            Field maxHitsField = vfbp.getClass().getDeclaredField("maxHits");
            maxHitsField.setAccessible(true);
            maxHitsField.set(vfbp, maxHits);

            Field maxKeysField = vfbp.getClass().getDeclaredField("maxKeys");
            maxKeysField.setAccessible(true);
            maxKeysField.set(vfbp, maxKeys);

            Field whitelistField = vfbp.getClass().getDeclaredField("whitelist");
            whitelistField.setAccessible(true);
            whitelistField.set(vfbp, whitelist);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }

        return vfbp.filter(val);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, bandPassFilter(null, 0, 0, 0, null));
    }

    @Test
    public void basicBand() {
        HashSet<String> whitelist = new HashSet<String>();
        whitelist.add("bar");
        assertEquals("foo", bandPassFilter("foo", 0, 0, 0, null));
        assertEquals(null, bandPassFilter("foo", 1, 1, 0, null));
        assertEquals("bar", bandPassFilter("bar", 1, 1, 0, whitelist));
    }
}

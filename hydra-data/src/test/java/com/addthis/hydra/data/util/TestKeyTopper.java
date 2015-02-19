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
package com.addthis.hydra.data.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestKeyTopper {

    @Test
    public void emptyBytesEncoding() {
        KeyTopper topper1 = new KeyTopper();
        topper1.init().setLossy(true);
        assertEquals(0, topper1.size());
        byte[] serialized = topper1.bytesEncode(0);
        assertEquals(0, serialized.length);
        KeyTopper topper2 = new KeyTopper();
        topper2.bytesDecode(serialized, 0);
        assertEquals(0, topper2.size());
    }

    @Test
    public void nonEmptyBytesEncoding() {
        KeyTopper topper1 = new KeyTopper();
        topper1.init().setLossy(true);
        topper1.increment("a", 1, 5);
        topper1.increment("b", 2, 5);
        topper1.increment("c", 3, 5);
        topper1.increment("d", 4, 5);
        assertEquals(4, topper1.size());
        byte[] serialized = topper1.bytesEncode(0);
        KeyTopper topper2 = new KeyTopper();
        topper2.bytesDecode(serialized, 0);
        assertEquals(4, topper2.size());
        assertEquals(new Long(1), topper2.get("a"));
        assertEquals(new Long(2), topper2.get("b"));
        assertEquals(new Long(3), topper2.get("c"));
        assertEquals(new Long(4), topper2.get("d"));
    }

    @Test
    public void minimumKey() {
        KeyTopper topper = new KeyTopper();
        topper.init().setLossy(true);
        topper.increment("100", 100, 100);
        assertNull(topper.getMinKey());
        for (int i = 99; i > 0; i--) {
            topper.increment(Integer.toString(i), i, 100);
            assertEquals(new Long(i), topper.get(Integer.toString(i)));
            assertEquals(Integer.toString(i), topper.getMinKey());
            assertEquals(i, topper.getMinVal());
        }
        String evicted = topper.increment("101", 101, 100);
        assertEquals("1", evicted);
        assertEquals(new Long(102), topper.get("101"));
        assertEquals("2", topper.getMinKey());
    }

    @Test
    public void incrementNoEviction() {
        KeyTopper topper = new KeyTopper();

        topper.init().setLossy(true);

        assertNull(topper.getMinKey());

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals("0", topper.getMinKey());
        assertEquals(1L, topper.getMinVal());

        assertEquals(100, topper.size());

        Long one = 1L;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        for (int i = 0; i < 100; i++) {
            assertEquals(new Long(2), topper.get(Integer.toString(i)));
        }

    }

    @Test
    public void incrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        topper.init().setLossy(true);

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = 1L;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 100);

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertNotNull(topper.getMinKey());
        assertNotEquals("0", topper.getMinKey());
        assertEquals(1L, topper.getMinVal());

        evicted = topper.increment(Integer.toString(101), 100);

        assertNull(evicted);

        assertEquals(100, topper.size());

        assertEquals(new Long(3), topper.get(Integer.toString(101)));


    }

    @Test
    public void weightedIncrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        topper.init().setLossy(true);

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = 1L;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 2, 100);

        assertEquals(100, topper.size());

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertEquals(new Long(3), topper.get(Integer.toString(101)));
    }

}

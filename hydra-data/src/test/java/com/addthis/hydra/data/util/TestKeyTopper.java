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

import java.util.Map;

import com.addthis.codec.CodecBin2;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestKeyTopper {

    @Test
    public void testConcurrentToSequentialCodableUpgrade() throws Exception {
        CodecBin2 codec = new CodecBin2();
        ConcurrentKeyTopper input = new ConcurrentKeyTopper();
        input.init();
        input.increment("a", 1, 5);
        input.increment("b", 2, 5);
        input.increment("c", 3, 5);
        input.increment("d", 4, 5);
        byte[] serialized = codec.encode(input);
        KeyTopper output = codec.decode(KeyTopper.class, serialized);
        assertEquals(4, output.size());
        assertEquals(new Long(1), output.get("a"));
        assertEquals(new Long(2), output.get("b"));
        assertEquals(new Long(3), output.get("c"));
        assertEquals(new Long(4), output.get("d"));
    }

    @Test
    public void testConcurrentToSequentialBytesCodableUpgrade() throws Exception {
        ConcurrentKeyTopper input = new ConcurrentKeyTopper();
        input.init();
        input.increment("a", 1, 5);
        input.increment("b", 2, 5);
        input.increment("c", 3, 5);
        input.increment("d", 4, 5);
        byte[] serialized = input.bytesEncode(0);
        KeyTopper output = new KeyTopper();
        output.bytesDecode(serialized, 0);
        assertEquals(4, output.size());
        assertEquals(new Long(1), output.get("a"));
        assertEquals(new Long(2), output.get("b"));
        assertEquals(new Long(3), output.get("c"));
        assertEquals(new Long(4), output.get("d"));
    }

    @Test
    public void testEmptyBytesCodable() throws Exception {
        KeyTopper input = new KeyTopper();
        byte[] serialized = input.bytesEncode(0);
        assertEquals(0, serialized.length);
        KeyTopper output = new KeyTopper();
        output.bytesDecode(serialized, 0);
        assertEquals(0, output.size());
    }

    @Test
    public void testNonEmptyBytesCodable() throws Exception {
        KeyTopper input = new KeyTopper();
        input.increment("a", 1, 5);
        input.increment("b", 2, 5);
        input.increment("c", 3, 5);
        input.increment("d", 4, 5);
        byte[] serialized = input.bytesEncode(0);
        KeyTopper output = new KeyTopper();
        output.bytesDecode(serialized, 0);
        assertEquals(4, output.size());
        assertEquals(new Long(1), output.get("a"));
        assertEquals(new Long(2), output.get("b"));
        assertEquals(new Long(3), output.get("c"));
        assertEquals(new Long(4), output.get("d"));
    }

    @Test
    public void testIncrementNoEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        assertNull(topper.get("foo"));

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long two = (long) 2;

        for (int i = 0; i < 100; i++) {
            assertEquals(two, topper.get(Integer.toString(i)));
        }

    }

    @Test
    public void testIncrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 100);

        assertNotNull(evicted);
        assertNotNull(topper.get(Integer.toString(101)));
        assertNull(topper.get(evicted));

        evicted = topper.increment(Integer.toString(101), 100);

        assertNull(evicted);

        assertEquals(new Long(3), topper.get(Integer.toString(101)));


    }

    @Test
    public void testWeightedIncrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        assertNull(topper.get("foo"));

        String evicted = topper.increment(Integer.toString(101), 2, 100);

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertEquals(new Long(3), topper.get(Integer.toString(101)));
    }

    @Test
    public void testGetSortedEntries() {
        KeyTopper topper = new KeyTopper();
        topper.increment("a", 40, 5);
        topper.increment("b", 30, 5);
        topper.increment("c", 20, 5);
        topper.increment("d", 10, 5);
        Map.Entry<String,Long>[] result = topper.getSortedEntries();
        assertEquals(4, result.length);
        assertEquals("d", result[0].getKey());
        assertEquals("c", result[1].getKey());
        assertEquals("b", result[2].getKey());
        assertEquals("a", result[3].getKey());
    }

    @Test
    public void testResize() {
        KeyTopper topper = new KeyTopper();
        topper.increment("a", 10, 5);
        topper.increment("b", 10, 1);
        assertEquals(1, topper.size());
        topper.increment("c", 10, 5);
        topper.increment("d", 10, 5);
        topper.increment("e", 10, 5);
        topper.increment("f", 10, 5);
        topper.increment("g", 10, 5);
        assertEquals(5, topper.size());
    }

}

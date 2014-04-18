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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestConcurrentKeyTopper {

    @Test
    public void testEmptyBytesEncoding() {
        ConcurrentKeyTopper topper1 = new ConcurrentKeyTopper();
        topper1.init();
        assertEquals(0, topper1.size());
        byte[] serialized = topper1.bytesEncode(0);
        assertEquals(0, serialized.length);
        ConcurrentKeyTopper topper2 = new ConcurrentKeyTopper();
        topper2.bytesDecode(serialized, 0);
        assertEquals(0, topper2.size());
    }

    @Test
    public void testNonEmptyBytesEncoding() {
        ConcurrentKeyTopper topper1 = new ConcurrentKeyTopper();
        topper1.init();
        topper1.increment("a", 1, 5);
        topper1.increment("b", 2, 5);
        topper1.increment("c", 3, 5);
        topper1.increment("d", 4, 5);
        assertEquals(4, topper1.size());
        byte[] serialized = topper1.bytesEncode(0);
        ConcurrentKeyTopper topper2 = new ConcurrentKeyTopper();
        topper2.bytesDecode(serialized, 0);
        assertEquals(4, topper2.size());
        assertEquals(new Long(1), topper2.get("a"));
        assertEquals(new Long(2), topper2.get("b"));
        assertEquals(new Long(3), topper2.get("c"));
        assertEquals(new Long(4), topper2.get("d"));
    }


    @Test
    public void testIncrementNoEviction() {
        ConcurrentKeyTopper topper = new ConcurrentKeyTopper();

        topper.init();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

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
        ConcurrentKeyTopper topper = new ConcurrentKeyTopper();

        topper.init();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 100);

        assertNull(evicted);
        assertNull(topper.get(Integer.toString(101)));

        evicted = topper.increment(Integer.toString(101), 100);

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertEquals(new Long(2), topper.get(Integer.toString(101)));


    }

    @Test
    public void testWeightedIncrementWithEviction() {
        ConcurrentKeyTopper topper = new ConcurrentKeyTopper();

        topper.init();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;
        Long two = (long) 2;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 2, 100);

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertEquals(two, topper.get(Integer.toString(101)));
    }

    private void insertElements(List<String> input, Map<String, Integer> count,
            int numElements, int numRepeats) {
        for (int i = 0; i < numElements; i++) {
            String nextString;
            do {
                nextString = RandomStringUtils.randomAlphabetic(20);
            }
            while (count.containsKey(nextString));

            count.put(nextString, numRepeats);

            for (int j = 0; j < numRepeats; j++) {
                input.add(nextString);
            }
        }
    }

    private static class InsertionThread extends Thread {

        private final ConcurrentKeyTopper topper;
        private final List<String> input;
        private final int threadId;
        private final int nThreads;

        InsertionThread(ConcurrentKeyTopper topper, List<String> input,
                int threadId, int nThreads) {
            this.topper = topper;
            this.input = input;
            this.threadId = threadId;
            this.nThreads = nThreads;
        }

        @Override
        public void run() {
            int length = input.size();
            int counter = 0;

            for (int i = 0; i < length; i++) {
                if (i % nThreads == threadId) {
                    topper.increment(input.get(i), 200);
                    counter++;
                }
            }

            assertTrue((counter * nThreads) < (length + nThreads));
            assertTrue((counter * nThreads) > (length - nThreads));
        }

    }

    @Test
    public void multithreadedIncrementWithEviction() {
        try {
            List<String> input = new ArrayList<>();
            Map<String, Integer> count = new HashMap<>();

            // insert 100 elements that appear 100 times each
            insertElements(input, count, 100, 100);
            // insert 100 elements that appear 10 times each
            insertElements(input, count, 100, 10);
            // insert 100 elements that appear 1 times each
            insertElements(input, count, 100, 1);

            Collections.shuffle(input);

            ConcurrentKeyTopper topper = new ConcurrentKeyTopper();

            topper.init();

            int numThreads = 8;
            InsertionThread[] threads = new InsertionThread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(topper, input, i, numThreads);
            }


            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            // all the elements with a count of 100 must appear in the topper
            for (Map.Entry<String, Integer> entry : count.entrySet()) {
                if (entry.getValue() == 100) {
                    assertNotNull(topper.get(entry.getKey()));
                }
            }

        } catch (InterruptedException ingored) {
            fail();
        }
    }


}

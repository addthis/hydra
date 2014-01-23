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
package com.addthis.hydra.store.skiplist;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.Files;

import com.addthis.hydra.store.kv.ConcurrentByteStoreBDB;
import com.addthis.hydra.store.kv.ExternalPagedStore.ByteStore;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("ConstantConditions")
public class TestSkipListCache {

    static final int fastNumElements = 10000;
    static final int fastNumThreads = 8;

    static final int slowNumElements = 100000;
    static final int slowNumThreads = 8;

    private void consistentWaitShutdown(SkipListCache cache) {
        cache.waitForShutdown();
    }

    private File makeTemporaryDirectory() throws IOException {
        final File temp;

        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return temp;
    }

    static final class InsertionThread extends Thread {

        final CyclicBarrier barrier;
        final List<Integer> values;
        final int[] threadId;
        final int myId;
        final SkipListCache<Integer, Integer> cache;
        int counter;

        public InsertionThread(CyclicBarrier barrier, List<Integer> values,
                int[] threadId, int id, SkipListCache<Integer, Integer> cache) {
            super("InsertionThread" + id);
            this.barrier = barrier;
            this.values = values;
            this.threadId = threadId;
            this.myId = id;
            this.cache = cache;
            this.counter = 0;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < threadId.length; i++) {
                    if (threadId[i] == myId) {
                        Integer val = values.get(i);
                        cache.put(val, threadId.length - val);
                        counter++;
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }

    }


    @Test
    public void testEmptyIntTree() {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25, 0).build();
            assertEquals(null, cache.get(Integer.MIN_VALUE));
            assertEquals(null, cache.get(0));
            assertEquals(null, cache.get(1));
            assertEquals(null, cache.get(2));
            cache.close();
        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    @Test
    public void testSimplePut() {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25, 0).build();

            for (int i = 0; i < 10000; i++) {
                assertEquals(null, cache.put(i, 10000 - i));
            }

            for (int i = 0; i < 10000; i++) {
                assertEquals(new Integer(10000 - i), cache.get(i));
            }

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());

        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }

    }

    private static final int rangeDeletionSlowIterations = 1_000;
    private static final int rangeDeletionSlowElements = 1_000;

    private static final int rangeDeletionFastIterations = 3;
    private static final int rangeDeletionFastElements = 100;

    @Test
    @Category(SlowTest.class)
    public void testRangeDeletionSlow() {
        rangeDeletionIterations(rangeDeletionSlowIterations, rangeDeletionSlowElements);
    }

    @Test
    public void testRangeDeletion() {
        rangeDeletionIterations(rangeDeletionFastIterations, rangeDeletionFastElements);
    }

    private void rangeDeletionIterations(int iterations, int elements) {
        Random generator = new Random();
        for (int i = 0; i < iterations; i++) {
            rangeDeletionIteration(generator.nextInt(elements),
                    generator.nextInt(elements), generator.nextBoolean(), elements);
        }

        for (int i = 0; i < iterations; i++) {
            rangeDeletionIteration(-1, generator.nextInt(elements), generator.nextBoolean(), elements);
        }

        for (int i = 0; i < iterations; i++) {
            rangeDeletionIteration(generator.nextInt(elements), elements, generator.nextBoolean(), elements);
        }


    }

    private void rangeDeletionIteration(int startKey, int endKey, boolean inclusive, int elements) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25, 0).build();

            for (int i = 0; i < elements; i++) {
                assertEquals(null, cache.put(i, elements - i));
            }

            cache.removeValues(startKey, endKey, inclusive);

            for (int i = 0; i < elements; i++) {
                if ((i >= startKey) &&
                    ((inclusive && i <= endKey) ||
                     (!inclusive && i < endKey))) {
                    assertNull(cache.get(i));
                } else {
                    assertEquals(new Integer(elements - i), cache.get(i));
                }
            }

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());

        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }

    }

    private void removePages(int numElements, int numThreads, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            cache.waitForPageEviction();

            assertEquals(new Integer(0), cache.getFirstKey());

            for (int i = 0; i < numElements; i++) {
                cache.remove(i);
            }

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void firstKey(int numElements, int numThreads, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            assertEquals(new Integer(0), cache.getFirstKey());

            for (int i = 0; i < numElements - 1; i++) {
                cache.remove(i);
            }

            assertEquals(new Integer(numElements - 1), cache.getFirstKey());

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void iterator(int numElements, int numThreads, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            Iterator<Map.Entry<Integer, Integer>> iterator = cache.range(0, true);

            for (int i = 0; i < numElements; i++) {
                assertTrue(iterator.hasNext());
                Map.Entry<Integer, Integer> entry = iterator.next();
                assertEquals(new Integer(i), entry.getKey());
                assertEquals(new Integer(numElements - i), entry.getValue());
            }

            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            iterator = cache.range(0, false);

            for (int i = 1; i < numElements; i++) {
                assertTrue(iterator.hasNext());
                Map.Entry<Integer, Integer> entry = iterator.next();
                assertEquals(new Integer(i), entry.getKey());
                assertEquals(new Integer(numElements - i), entry.getValue());
            }

            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            iterator = cache.range(numElements, true);
            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            iterator = cache.range(numElements, false);
            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void lastKey(int numElements, int numThreads, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            assertEquals(new Integer(numElements - 1), cache.getLastKey());

            for (int i = 1; i < numElements; i++) {
                cache.remove(i);
            }

            assertEquals(new Integer(0), cache.getLastKey());

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void multiThreadedInsert(int numElements, int numThreads, int pageSize) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void doTestMultiThreadedInsertSmallPages(int numElements, int numThreads) {
        multiThreadedInsert(numElements, numThreads, 4);
    }

    private void doTestMultiThreadedInsertLargePages(int numElements, int numThreads) {
        multiThreadedInsert(numElements, numThreads, 128);
    }

    private void doTestMaxPages(int numElements, int numThreads) {
        File directory = null;

        try {

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            Thread[] threads = new Thread[numThreads];

            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 8, 50).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            try {
                for (int i = 0; i < numThreads; i++) {
                    threads[i].join();
                }
            } catch (InterruptedException e) {
                fail();
            }

            for (int i = 0; i < numElements; i++) {
                Integer value = cache.getValue(i);
                assertNotNull(value);
                assertEquals(new Integer(numElements - i), value);
            }

            cache.waitForPageEviction();

            assertTrue(cache.getNumPagesInMemory() >= 30);
            assertTrue(cache.getNumPagesInMemory() <= 50);

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getNumPagesInMemory());
            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }


    private void doTestBackgroundEvictionThread(int numElements, int numThreads) {
        File directory = null;

        try {

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            Thread[] threads = new Thread[numThreads];

            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 8, 0).build();

            cache.setOverrideDefaultMaxPages();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }

            try {
                for (int i = 0; i < numThreads; i++) {
                    threads[i].join();
                }
            } catch (InterruptedException e) {
                fail();
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            cache.waitForPageEviction();

            assertEquals(0, cache.getNumPagesInMemory());
            assertEquals(0, cache.getMemoryEstimate());

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getNumPagesInMemory());
            assertEquals(0, cache.getMemoryEstimate());
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!Files.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void doTestExternalStorePersistance(int numElements) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            SkipListCache<Integer, Integer> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 8, Integer.MAX_VALUE).build();


            for (int i = 0; i < numElements; i++) {
                cache.put(i, numElements - i);
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            consistentWaitShutdown(cache);

            assertEquals(0, cache.getNumPagesInMemory());
            assertEquals(0, cache.getMemoryEstimate());

            externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            cache = new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 8, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i));
            }

            Thread.sleep(100);

            consistentWaitShutdown(cache);

        } catch (IOException | InterruptedException ex) {
            fail();
        } finally {
            if (directory != null) {
                Files.deleteDir(directory);
            }
        }

    }

    @Test
    public void testExternalStorePersistance() {
        doTestExternalStorePersistance(fastNumElements);
        doTestExternalStorePersistance(4);
    }

    @Test
    public void testMaxPages() {
        doTestMaxPages(fastNumElements, fastNumThreads);
    }

    @Test
    public void testBackgroundEvictionThread() {
        doTestBackgroundEvictionThread(fastNumElements, fastNumThreads);
    }

    @Test
    public void testRemoveLargePages() {
        removePages(fastNumElements, fastNumThreads, 128, Integer.MAX_VALUE);
    }

    @Test
    public void testMultiThreadedInsertLargePages() {
        doTestMultiThreadedInsertLargePages(fastNumElements, fastNumThreads);
    }

    @Test
    public void testIteratorLargePages() {
        iterator(fastNumElements, fastNumThreads, 128, Integer.MAX_VALUE);
    }

    @Test
    @Category(SlowTest.class)
    public void testExternalStorePersistanceSlow() {
        doTestExternalStorePersistance(slowNumElements);
    }

    @Test
    @Category(SlowTest.class)
    public void testMaxPagesSlow() {
        doTestMaxPages(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testBackgroundEvictionThreadSlow() {
        doTestBackgroundEvictionThread(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testIteratorSmallPagesSlow() {
        iterator(slowNumElements, slowNumThreads, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testIteratorLargePagesSlow() {
        iterator(slowNumElements, slowNumThreads, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testLastKeyLargePagesSlow() {
        lastKey(slowNumElements, slowNumThreads, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedInsertSmallPagesSlow() {
        doTestMultiThreadedInsertSmallPages(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testFirstKeySmallPagesSlow() {
        firstKey(slowNumElements, slowNumThreads, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testFirstKeyLargePagesSlow() {
        firstKey(slowNumElements, slowNumThreads, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testLastKeySmallPagesSlow() {
        lastKey(slowNumElements, slowNumThreads, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testRemoveLargePagesSlow() {
        removePages(slowNumElements, slowNumThreads, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testRemoveSmallPagesSlow() {
        removePages(slowNumElements, slowNumThreads, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedInsertSmallPagesIterations() {
        for (int i = 0; i < 100; i++) {
            doTestMultiThreadedInsertSmallPages(fastNumElements, fastNumThreads);
        }
    }

}

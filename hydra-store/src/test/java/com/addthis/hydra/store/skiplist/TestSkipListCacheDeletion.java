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
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.Files;

import com.addthis.hydra.store.DBIntValue;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.ConcurrentByteStoreBDB;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@SuppressWarnings("ConstantConditions")
public class TestSkipListCacheDeletion {

    static final int insertFraction = 4;

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
        final SkipListCache<Integer, DBIntValue> cache;
        int counter;

        public InsertionThread(CyclicBarrier barrier, List<Integer> values,
                int[] threadId, int id, SkipListCache<Integer, DBIntValue> cache) {
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
                        if (val % insertFraction == 0) {
                            cache.put(val, new DBIntValue(2 * val));
                        }
                        counter++;
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }

    }

    static final class DeletionThread extends Thread {

        final CyclicBarrier barrier;
        final List<Integer> values;
        final int[] threadId;
        final int myId;
        final SkipListCache<Integer, DBIntValue> cache;
        int counter;

        public DeletionThread(CyclicBarrier barrier, List<Integer> values,
                int[] threadId, int id, SkipListCache<Integer, DBIntValue> cache) {
            super("DeletionThread" + id);
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
                        if ((val % insertFraction) != 0) {
                            cache.remove(val);
                        }
                        counter++;
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }

    }

    private void multiThreadedDelete(int numElements, int numThreads, int pageSize) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] insertionThreads = new InsertionThread[numThreads];
            DeletionThread[] deletionThreads = new DeletionThread[numThreads];
            SkipListCache<Integer, DBIntValue> cache =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, pageSize, 1000).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                cache.put(i, new DBIntValue(i));
                threadId[i] = i % numThreads;
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                insertionThreads[i] = new InsertionThread(barrier, values, threadId, i, cache);
                deletionThreads[i] = new DeletionThread(barrier, values, threadId, i, cache);
            }

            Collections.shuffle(values);

            for (int i = 0; i < numThreads; i++) {
                deletionThreads[i].start();
                insertionThreads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                deletionThreads[i].join();
                insertionThreads[i].join();
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, deletionThreads[i].counter);
                assertEquals(numElements / numThreads, insertionThreads[i].counter);
            }

            // System.out.println("Number pages deleted: " + cache.getNumPagesDeleted());
            // System.out.println("Number pages in memory: " + cache.getNumPagesInMemory());
            // System.out.println("Number pages on disk: " + cache.getNumPagesOnDisk());

            for (int i = 0; i < numElements; i++) {
                if (i % insertFraction == 0) {
                    assertEquals(new Integer(2 * i), cache.get(i).getVal());
                } else {
                    assertNull(cache.get(i));
                }
            }

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

    private void doTestMultiThreadedDeleteSmallPages(int numElements, int numThreads) {
        multiThreadedDelete(numElements, numThreads, 4);
    }

    private void doTestMultiThreadedDeleteLargePages(int numElements, int numThreads) {
        multiThreadedDelete(numElements, numThreads, 128);
    }

    @Test
    public void testMultiThreadedDeleteLargePages() {
        doTestMultiThreadedDeleteLargePages(fastNumElements, fastNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedDeleteSmallPagesSlow() {
        doTestMultiThreadedDeleteSmallPages(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedDeleteSmallPagesIterations() {
        for (int i = 0; i < 100; i++) {
            doTestMultiThreadedDeleteSmallPages(fastNumElements, fastNumThreads);
        }
    }

}

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
package com.addthis.hydra.store.kv;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.Files;

import com.addthis.hydra.store.skiplist.SimpleIntKeyCoder;
import com.addthis.hydra.store.skiplist.SkipListCache;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestKeyValueInteroperability {

    static final int fastNumElements = 1000;
    static final int fastNumThreads = 8;

    static final int slowNumElements = 10000;
    static final int slowNumThreads = 8;

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

    private void doTestExternalPagedStoreToSkipListCache(int numElements, int numThreads) {
        File directory = null;

        try {

            directory = makeTemporaryDirectory();
            ExternalPagedStore.ByteStore externalStore = new ByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<Integer>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            PagedKeyValueStore<Integer, Integer> tree = new ExternalPagedStore<>(new SimpleIntKeyCoder(),
                    externalStore, 64, Integer.MAX_VALUE);
            tree.setMaxTotalMem(Integer.MAX_VALUE);

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, tree);
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

            System.out.println("EPS size is " + ((ExternalPagedStore) tree).getMemoryEstimate());

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            tree.close();

            externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            tree = new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            System.out.println("skip list size is " + ((SkipListCache) tree).getMemoryEstimate());

            tree.close();

        } catch (IOException ex) {
            fail();
        } finally {
            if (directory != null) {
                Files.deleteDir(directory);
            }
        }


    }

    private void doTestSkipListCacheToExternalPagedStore(int numElements, int numThreads) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ExternalPagedStore.ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<Integer>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            PagedKeyValueStore<Integer, Integer> tree =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, tree);
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

            System.out.println("skip list size is " + ((SkipListCache) tree).getMemoryEstimate());

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            tree.close();

            externalStore = new ByteStoreBDB(directory, "db", false);

            tree = new ExternalPagedStore<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE);
            tree.setMaxTotalMem(Integer.MAX_VALUE);

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            System.out.println("EPS size is " + ((ExternalPagedStore) tree).getMemoryEstimate());

            tree.close();

        } catch (IOException ex) {
            fail();
        } finally {
            if (directory != null) {
                Files.deleteDir(directory);
            }
        }


    }

    private void doTestExternalPagedStoreToExternalPagedStore(int numElements, int numThreads) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ExternalPagedStore.ByteStore externalStore = new ByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<Integer>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            PagedKeyValueStore<Integer, Integer> tree = new ExternalPagedStore<>(new SimpleIntKeyCoder(),
                    externalStore, 64, Integer.MAX_VALUE);
            tree.setMaxTotalMem(Integer.MAX_VALUE);

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, tree);
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

            long firstMemoryEstimate = ((ExternalPagedStore) tree).getMemoryEstimate();

            System.out.println("EPS size is " + firstMemoryEstimate);

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            tree.close();

            externalStore = new ByteStoreBDB(directory, "db", false);

            tree = new ExternalPagedStore<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE);
            tree.setMaxTotalMem(Integer.MAX_VALUE);

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            long secondMemoryEstimate = ((ExternalPagedStore) tree).getMemoryEstimate();

            System.out.println("EPS size is " + secondMemoryEstimate);

            tree.close();

        } catch (IOException ex) {
            fail();
        } finally {
            if (directory != null) {
                Files.deleteDir(directory);
            }
        }


    }

    private void doTestSkipListCacheToSkipListCache(int numElements, int numThreads) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ExternalPagedStore.ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            ArrayList<Integer> values = new ArrayList<Integer>(numElements);
            final CyclicBarrier barrier = new CyclicBarrier(numThreads);
            int[] threadId = new int[numElements];
            InsertionThread[] threads = new InsertionThread[numThreads];
            PagedKeyValueStore<Integer, Integer> tree =
                    new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
                threadId[i] = i % numThreads;
            }

            for (int i = 0; i < numThreads; i++) {
                threads[i] = new InsertionThread(barrier, values, threadId, i, tree);
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

            long firstMemoryEstimate = ((SkipListCache) tree).getMemoryEstimate();

            System.out.println("skip list size is " + firstMemoryEstimate);

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            for (int i = 0; i < numThreads; i++) {
                assertEquals(numElements / numThreads, threads[i].counter);
            }

            tree.close();

            externalStore = new ConcurrentByteStoreBDB(directory, "db", false);

            tree = new SkipListCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 64, Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(i), tree.getValue(i));
            }

            long secondMemoryEstimate = ((SkipListCache) tree).getMemoryEstimate();

            System.out.println("skip list size is " + secondMemoryEstimate);

            assertEquals(firstMemoryEstimate, secondMemoryEstimate);

            tree.close();

        } catch (IOException ex) {
            fail();
        } finally {
            if (directory != null) {
                Files.deleteDir(directory);
            }
        }


    }


    static final class InsertionThread extends Thread {

        final CyclicBarrier barrier;
        final List<Integer> values;
        final int[] threadId;
        final int myId;
        final PagedKeyValueStore<Integer, Integer> tree;
        int counter;

        public InsertionThread(CyclicBarrier barrier, List<Integer> values,
                int[] threadId, int id, PagedKeyValueStore<Integer, Integer> tree) {
            this.barrier = barrier;
            this.values = values;
            this.threadId = threadId;
            this.myId = id;
            this.tree = tree;
            this.counter = 0;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < threadId.length; i++) {
                    if (threadId[i] == myId) {
                        Integer val = values.get(i);
                        tree.putValue(val, val);
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
    public void testSkipListCacheToExternalPagedStore() {
        doTestSkipListCacheToExternalPagedStore(fastNumElements, fastNumThreads);
    }

    @Test
    public void testExternalPagedStoreToSkipListCache() {
        doTestExternalPagedStoreToSkipListCache(fastNumElements, fastNumThreads);
    }

    @Test
    public void testExternalPagedStoreToExternalPagedStore() {
        doTestExternalPagedStoreToExternalPagedStore(fastNumElements, fastNumThreads);
    }

    @Test
    public void testSkipListCacheToSkipListCache() {
        doTestSkipListCacheToSkipListCache(fastNumElements, fastNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testSkipListCacheToExternalPagedStoreSlow() {
        doTestSkipListCacheToExternalPagedStore(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testExternalPagedStoreToSkipListCacheSlow() {
        doTestExternalPagedStoreToSkipListCache(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testExternalPagedStoreToExternalPagedStoreSlow() {
        doTestExternalPagedStoreToExternalPagedStore(slowNumElements, slowNumThreads);
    }

    @Test
    @Category(SlowTest.class)
    public void testSkipListCacheToSkipListCacheSlow() {
        doTestSkipListCacheToSkipListCache(slowNumElements, slowNumThreads);
    }


}

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
package com.addthis.hydra.store.nonconcurrent;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.LessFiles;
import com.addthis.hydra.store.DBIntValue;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.kv.ByteStore;
import com.addthis.hydra.store.kv.ConcurrentByteStoreBDB;
import com.addthis.hydra.store.skiplist.SimpleIntKeyCoder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class TestNonConcurrentPageCache {

    static final int fastNumElements = 10000;

    static final int slowNumElements = 100000;

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


    @Test
    public void testEmptyIntTree() {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25).build();
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
                if (!LessFiles.deleteDir(directory)) {
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
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25).build();

            for (int i = 0; i < 10000; i++) {
                assertEquals(null, cache.put(i, new DBIntValue(10000 - i)));
            }

            for (int i = 0; i < 10000; i++) {
                assertEquals(new Integer(10000 - i), cache.get(i).getVal());
            }

            cache.doClose(false, CloseOperation.NONE);

        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
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
                    generator.nextInt(elements), elements);
        }

        for (int i = 0; i < iterations; i++) {
            rangeDeletionIteration(-1, generator.nextInt(elements), elements);
        }

        for (int i = 0; i < iterations; i++) {
            rangeDeletionIteration(generator.nextInt(elements), elements, elements);
        }


    }

    private void rangeDeletionIteration(int startKey, int endKey, int elements) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore, 25).build();

            for (int i = 0; i < elements; i++) {
                assertEquals(null, cache.put(i, new DBIntValue(elements - i)));
            }

            cache.removeValues(startKey, endKey);

            for (int i = 0; i < elements; i++) {
                if ((i >= startKey) &&
                        (i < endKey)) {
                    assertNull(cache.get(i));
                } else {
                    assertEquals(new Integer(elements - i), cache.get(i).getVal());
                }
            }

            cache.doClose(false, CloseOperation.NONE);

        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }

    }

    private void removePages(int numElements, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            pageSize).maxPages(maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }

            Collections.shuffle(values);


            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            cache.pushAllPagesToDisk();

            assertEquals(new Integer(0), cache.getFirstKey());

            for (int i = 0; i < numElements; i++) {
                cache.remove(i);
            }


            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void firstKey(int numElements, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            pageSize).maxPages(maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }

            Collections.shuffle(values);

            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            assertEquals(new Integer(0), cache.getFirstKey());

            for (int i = 0; i < numElements - 1; i++) {
                cache.remove(i);
            }

            assertEquals(new Integer(numElements - 1), cache.getFirstKey());


            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void iterator(int numElements, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            pageSize).maxPages(maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }

            Collections.shuffle(values);


            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            Iterator<Map.Entry<Integer, DBIntValue>> iterator = cache.range(0);

            for (int i = 0; i < numElements; i++) {
                assertTrue(iterator.hasNext());
                Map.Entry<Integer, DBIntValue> entry = iterator.next();
                assertEquals(new Integer(i), entry.getKey());
                assertEquals(new Integer(numElements - i), entry.getValue().getVal());
            }

            assertFalse(iterator.hasNext());
            assertNull(iterator.next());

            iterator = cache.range(numElements);
            assertFalse(iterator.hasNext());
            assertNull(iterator.next());


            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void lastKey(int numElements, int pageSize, int maxPages) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            pageSize).maxPages(maxPages).build();

            if (maxPages == 0) {
                cache.setOverrideDefaultMaxPages();
            }

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }


            Collections.shuffle(values);

            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            assertEquals(new Integer(numElements - 1), cache.getLastKey());

            for (int i = 1; i < numElements; i++) {
                cache.remove(i);
            }

            assertEquals(new Integer(0), cache.getLastKey());


            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void multiThreadedInsert(int numElements, int pageSize) {

        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            ArrayList<Integer> values = new ArrayList<>(numElements);
            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            pageSize).maxPages(Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }

            Collections.shuffle(values);


            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }


            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void doTestMultiThreadedInsertSmallPages(int numElements) {
        multiThreadedInsert(numElements, 4);
    }

    private void doTestMultiThreadedInsertLargePages(int numElements) {
        multiThreadedInsert(numElements, 128);
    }

    private void doTestMaxPages(int numElements) {
        File directory = null;

        try {

            ArrayList<Integer> values = new ArrayList<>(numElements);

            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            8).maxPages(50).build();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }

            Collections.shuffle(values);


            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                Integer value = cache.getValue(i).getVal();
                assertNotNull(value);
                assertEquals(new Integer(numElements - i), value);
            }


            assertTrue(cache.getNumPagesInMemory() < 50);

            cache.pushAllPagesToDisk();

            assertEquals(0, cache.getNumPagesInMemory());
            cache.doClose(false, CloseOperation.NONE);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }


    private void doTestBackgroundEvictionThread(int numElements) {
        File directory = null;

        try {

            ArrayList<Integer> values = new ArrayList<>(numElements);

            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(),
                            externalStore, 8).maxPages(0).build();

            cache.setOverrideDefaultMaxPages();

            for (int i = 0; i < numElements; i++) {
                values.add(i);
            }
            Collections.shuffle(values);

            for (Integer value : values) {
                cache.put(value, new DBIntValue(numElements - value));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            cache.pushAllPagesToDisk();


            assertEquals(0, cache.getNumPagesInMemory());
            cache.doClose(false, CloseOperation.NONE);

        } catch (Exception ex) {
            fail();
        } finally {
            if (directory != null) {
                if (!LessFiles.deleteDir(directory)) {
                    fail();
                }
            }
        }
    }

    private void doTestExternalStorePersistance(int numElements) {
        File directory = null;

        try {
            directory = makeTemporaryDirectory();
            ByteStore externalStore = new ConcurrentByteStoreBDB(directory, "db");

            NonConcurrentPageCache<Integer, DBIntValue> cache =
                    new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                            8).maxPages(Integer.MAX_VALUE).build();


            for (int i = 0; i < numElements; i++) {
                cache.put(i, new DBIntValue(numElements - i));
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            cache.pushAllPagesToDisk();

            assertEquals(0, cache.getNumPagesInMemory());
            cache.doClose(false, CloseOperation.NONE);

            externalStore = new ConcurrentByteStoreBDB(directory, "db");

            cache = new NonConcurrentPageCache.Builder<>(new SimpleIntKeyCoder(), externalStore,
                    8).maxPages(Integer.MAX_VALUE).build();

            for (int i = 0; i < numElements; i++) {
                assertEquals(new Integer(numElements - i), cache.get(i).getVal());
            }

            Thread.sleep(100);


        } catch (IOException | InterruptedException ex) {
            fail();
        } finally {
            if (directory != null) {
                LessFiles.deleteDir(directory);
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
        doTestMaxPages(fastNumElements);
    }

    @Test
    public void testBackgroundEvictionThread() {
        doTestBackgroundEvictionThread(fastNumElements);
    }

    @Test
    public void testRemoveLargePages() {
        removePages(fastNumElements, 128, Integer.MAX_VALUE);
    }

    @Test
    public void testMultiThreadedInsertLargePages() {
        doTestMultiThreadedInsertLargePages(fastNumElements);
    }

    @Test
    public void testIteratorLargePages() {
        iterator(fastNumElements, 128, Integer.MAX_VALUE);
    }

    @Test
    @Category(SlowTest.class)
    public void testExternalStorePersistanceSlow() {
        doTestExternalStorePersistance(slowNumElements);
    }

    @Test
    @Category(SlowTest.class)
    public void testMaxPagesSlow() {
        doTestMaxPages(slowNumElements);
    }

    @Test
    @Category(SlowTest.class)
    public void testBackgroundEvictionThreadSlow() {
        doTestBackgroundEvictionThread(slowNumElements);
    }

    @Test
    @Category(SlowTest.class)
    public void testIteratorSmallPagesSlow() {
        iterator(slowNumElements, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testIteratorLargePagesSlow() {
        iterator(slowNumElements, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testLastKeyLargePagesSlow() {
        lastKey(slowNumElements, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedInsertSmallPagesSlow() {
        doTestMultiThreadedInsertSmallPages(slowNumElements);
    }

    @Test
    @Category(SlowTest.class)
    public void testFirstKeySmallPagesSlow() {
        firstKey(slowNumElements, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testFirstKeyLargePagesSlow() {
        firstKey(slowNumElements, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testLastKeySmallPagesSlow() {
        lastKey(slowNumElements, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testRemoveLargePagesSlow() {
        removePages(slowNumElements, 128, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testRemoveSmallPagesSlow() {
        removePages(slowNumElements, 4, 0);
    }

    @Test
    @Category(SlowTest.class)
    public void testMultiThreadedInsertSmallPagesIterations() {
        for (int i = 0; i < 100; i++) {
            doTestMultiThreadedInsertSmallPages(fastNumElements);
        }
    }

}

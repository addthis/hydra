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
package com.addthis.hydra.data.io;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.io.DiskBackedList2.ItemCodec;

import com.google.common.io.Files;

import org.apache.commons.io.output.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(SlowTest.class)
public class TestDiskBackedList2 {

    private final TestValueCodec codec = new TestValueCodec();

    public static class TestValueCodec implements ItemCodec<TestValue> {

        @Override
        public TestValue decode(byte[] row) throws IOException {
            ByteArrayInputStream in = new ByteArrayInputStream(row);
            TestValue tv = new TestValue();
            tv.value = LessBytes.readLong(in);
            tv.randomBytes = LessBytes.readBytes(in);
            return tv;
        }

        @Override
        public byte[] encode(TestValue row) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            LessBytes.writeLong(row.value, out);
            LessBytes.writeBytes(row.randomBytes, out);
            return out.toByteArray();
        }
    }

    public static final class TestValue implements Codable {

        private static final Random random = new Random(1234);
        @FieldConfig(codable = true)
        private long value;
        @FieldConfig(codable = true)
        private byte[] randomBytes;

        public TestValue() {
        }

        public TestValue(long val) {
            this.value = val;
            this.randomBytes = new byte[random.nextInt() & 0xff];
            random.nextBytes(randomBytes);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof TestValue) && ((TestValue) o).value == value;
        }

        @Override
        public String toString() {
            return "TV:" + value + ":" + randomBytes.length;
        }
    }

    @Test
    public void addRemoveSizeTest() throws IOException {
        Random random = new Random(58);
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec);
        try {
            int numEntries = 1000;
            int[] vals = new int[numEntries];
            for (int i = 0; i < numEntries; i++) {
                vals[i] = random.nextInt(3 * numEntries);
            }
            // Add a bunch of entries and make sure they all get set correctly
            for (int i = 0; i < numEntries; i++) {
                dbl.add(new TestValue(vals[i]));
                Assert.assertEquals(vals[i], dbl.get(i).value);
            }
            // Make sure resulting size is correct
            Assert.assertEquals(numEntries, dbl.size());
            // Remove half of the entries, check if new size is correct
            for (int i = (numEntries / 2); i > 0; i--) {
                dbl.remove(i);
            }
            Assert.assertEquals(numEntries - numEntries / 2, dbl.size());
            dbl.clear();
        } finally {
            LessFiles.deleteDir(dbl.getDirectory());
        }
    }

    @Test
    public void addAtIndexTest() throws IOException {
        List<Integer> ints = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5));
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec);
        try {
            // Add a bunch of entries and make sure they all get set correctly
            for (Integer z : ints) {
                dbl.add(new TestValue(z));
            }
            ints.add(2, 100);
            dbl.add(2, new TestValue(100));
            int i = 0;
            for (TestValue tv : dbl) {
                Assert.assertEquals(new TestValue(ints.get(i++)), tv);
            }
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void canHoldBigLoadTest() throws IOException {
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec);
        try {
            int numEntries = 1000000;
            // Add a large number of entries to make sure we don't overflow heap
            for (int i = 0; i < numEntries; i++) {
                dbl.add(new TestValue(i));
            }
            // Make sure resulting size is correct
            Assert.assertEquals(numEntries, dbl.size());
            // Make sure a random read from the middle gives the correct value
            int middleEntry = numEntries / 2;
            TestValue tv = dbl.get(middleEntry);
            Assert.assertEquals(middleEntry, tv.value);
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void iteratorTest() throws IOException {
        // Test whether the iterator has the correct set of values and no extras
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec);
        try {
            int numEntries = 10000;
            for (int i = 0; i < numEntries; i++) {
                dbl.add(new TestValue(i));
            }
            Iterator<TestValue> listIter = dbl.listIterator();
            for (int i = 0; i < numEntries; i++) {
                Assert.assertEquals(new TestValue(i), listIter.next());
            }
            Assert.assertEquals(false, listIter.hasNext());
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void sortTest() throws IOException {
        // Manually sort a bunch of values, then make sure dbl.sort reproduces the same list
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec);
        try {
            Random random = new Random(1984);
            int numEntries = 20000;
            int[] vals = new int[numEntries];
            for (int i = 0; i < numEntries; i++) {
                vals[i] = random.nextInt(3 * numEntries);
            }
            int[] sortedVals = vals.clone();
            Arrays.sort(sortedVals);
            for (int val : vals) {
                dbl.add(new TestValue(val));
            }
            Comparator<TestValue> comp = new Comparator<TestValue>() {
                @Override
                public int compare(TestValue o1, TestValue o2) {
                    return (int) (o1.value - o2.value);
                }
            };
            dbl.sort(comp);
            int i = 0;
            for (TestValue tv : dbl) {
                Assert.assertEquals(sortedVals[i++], tv.value);
            }
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void dontExceedMaxDiskSpaceTest() throws IOException {
        System.setProperty("max.total.query.size.bytes", "200");
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec, 50, Files.createTempDir());
        try {
            boolean failed = false;
            for (int i = 0; i < 100; i++) {
                try {
                    dbl.add(new TestValue(i));
                } catch (RuntimeException ex) {
                    System.out.println(ex);
                    failed = true;
                    break;
                }
            }
            Assert.assertEquals(true, failed);
            System.clearProperty("max.total.query.size.bytes");
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void runStressTests() throws IOException {
        // This test can be used to measure sort runtimes
        // stressTest(16000000,10000000);
    }

    public void stressTest(int chunkSize, int numEntries) throws IOException {
        long startTime = System.currentTimeMillis();
        DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec, chunkSize, Files.createTempDir());
        try {
            int seed = 58;
            Random random = new Random(seed);
            for (int i = 0; i < numEntries; i++) {
                dbl.add(new TestValue(random.nextInt(3 * numEntries)));
            }
            Comparator<TestValue> comp = new Comparator<TestValue>() {
                @Override
                public int compare(TestValue o1, TestValue o2) {
                    return (int) (o1.value - o2.value);
                }
            };

            dbl.sort(comp);
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("chunkSize " + chunkSize + " numEntries " + numEntries + " took " + totalTime + " ms");
            dbl.clear();
        } finally {
            if (dbl != null) {
                LessFiles.deleteDir(dbl.getDirectory());
            }
        }
    }

    @Test
    public void saveToDirectoryTest() throws IOException {
        Random random = new Random(451);
        int numEntries = 100;
        int sizeBytes = 3000;
        File directory = Files.createTempDir();
        try {
            DiskBackedList2<TestValue> dbl = new DiskBackedList2<>(codec, sizeBytes, directory);
            for (int i = 0; i < numEntries; i++) {
                dbl.add(new TestValue(random.nextInt(2101)));
            }
            dbl.saveAllChunksToDirectory();
            DiskBackedList2<TestValue> anotherdbl = new DiskBackedList2<>(codec, sizeBytes, directory);
            anotherdbl.loadAllChunksFromDirectory();
            Assert.assertEquals(dbl.size(), anotherdbl.size());
            for (int i = 0; i < numEntries; i++) {
                Assert.assertEquals(dbl.get(i).value, anotherdbl.get(i).value);
            }
            dbl.clear();
        } finally {
            if (directory != null) {
                LessFiles.deleteDir(directory);
            }
        }
    }
}

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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.Files;

import com.addthis.hydra.store.DBValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(SlowTest.class)
public class TestKeyValueStore {

    static {
//		Debug.setLevel(1);
    }

    private static final Logger log = LoggerFactory.getLogger(TestKeyValueStore.class);

    /**
     * sort array of string tuples
     *
     * @param s raw string array of string tuples
     * @return sorted array
     */
    private static final String[][] sort(String s[][]) {
        TreeMap<String, String> map = new TreeMap<String, String>();
        for (String kv[] : s) {
            map.put(kv[0], kv[1]);
        }
        String ret[][] = new String[map.size()][];
        int pos = 0;
        for (Entry<String, String> e : map.entrySet()) {
            ret[pos++] = new String[]{e.getKey(), e.getValue()};
        }
        return ret;
    }

    /**
     * raw, unsorted sample data
     */
    protected static final String source[][] = new String[][]{
            new String[]{"aaa", "aaa"},
            new String[]{"ddd", "ddd"},
            new String[]{"ggg", "ggg"},
            new String[]{"000", "000"},
            new String[]{"111", "111"},
            new String[]{"333", "333"},
            new String[]{"666", "666"},
            new String[]{"ccc", "ccc"},
            new String[]{"555", "555"},
            new String[]{"xxx", "xxx"},
            new String[]{"yyy", "yyy"},
            new String[]{"zzz", "zzz"},
            new String[]{"eee", "eee"},
    };

    /**
     * sorted version of sample data
     */
    protected static final String sorted[][] = sort(source);

    /**
     * simple insert, iterate test of sorting with initial empty store
     */
    private void insertIterateTest(KeyValueStore<String, DBValue> store) {
        for (int i = 0; i < source.length; i++) {
            if (log.isDebugEnabled()) log.debug("iit.ins i=" + i + " source=" + source[i][0] + "," + source[i][1]);
            Assert.assertEquals(null, store.getPutValue(source[i][0], new DBValue(source[i][1])));
        }
        iterateTest(store);
    }

    /**
     * insert MUST run first
     */
    private void iterateTest(KeyValueStore<String, DBValue> store) {
        for (int start = 0; start < sorted.length - 1; start++) {
            if (log.isDebugEnabled()) log.debug("iit.rat | start=" + start);
            int pos = start;
            Iterator<Entry<String, DBValue>> i = store.range(sorted[start][0], true);
            while (i.hasNext()) {
                Entry<String, DBValue> e = i.next();
                String row[] = sorted[pos++];
                if (log.isDebugEnabled()) log.debug("iit.tst | e=" + e + " row=" + row[0] + "," + row[1] + " pos=" + pos);
                Assert.assertEquals(row[0], e.getKey());
                Assert.assertEquals(row[1], e.getValue().getVal());
                Assert.assertEquals(e.getKey(), e.getValue().getVal());
            }
        }
    }

    /**
     * insert, delete odd, iterate test with initial empty store
     */
    private void insertDeleteOddIterateTest(KeyValueStore<String, DBValue> store) {
        for (int i = 0; i < source.length; i++) {
            if (log.isDebugEnabled()) log.debug("idoit.ins i=" + i + " source=" + source[i][0] + "," + source[i][1]);
            Assert.assertEquals(null, store.getPutValue(source[i][0], new DBValue(source[i][1])));
        }
        for (int i = 1; i < sorted.length; i += 2) {
            if (log.isDebugEnabled()) log.debug("idoit.del i=" + i + " sorted=" + sorted[i][0] + "," + sorted[i][1]);
            Assert.assertEquals(sorted[i][1], store.getRemoveValue(sorted[i][0]));
            Assert.assertTrue(store.getValue(sorted[i][0]) == null);
        }
        int pos = 0;
        Iterator<Entry<String, DBValue>> i = store.range("000", true);
        while (i.hasNext()) {
            Entry<String, DBValue> e = i.next();
            String row[] = sorted[pos];
            if (log.isDebugEnabled()) log.debug("idoit | e=" + e + " row=" + row[0] + "," + row[1] + " pos=" + pos);
            Assert.assertEquals(row[0], e.getKey());
            Assert.assertEquals(row[1], e.getValue().getVal());
            Assert.assertEquals(e.getKey(), e.getValue().getVal());
            pos += 2;
        }
    }

    private void rangeCompare(String msg, Iterator<Entry<String, DBValue>> r1, Iterator<Entry<String, DBValue>> r2) {
        int iter = 0;
        while (r1.hasNext()) {
            String r1k = r1.next().getKey();
            if (log.isDebugEnabled()) log.debug("checking r2 for " + r1k);
            Assert.assertTrue(msg + ",nkm=" + r1k + ",iter=" + iter + ":missing next", r2.hasNext());
            String r2k = r2.next().getKey();
            Assert.assertEquals(msg, r1k, r2k);
            iter++;
        }
    }

    private void randomInsertRangeRead(KeyValueStore<String, DBValue> store) {
        TreeMap<String, DBValue> map = new TreeMap<>();
        Random rand = new Random(1000);
        // random insert
        if (log.isDebugEnabled()) log.debug("inserting 1000 random");
        for (int i = 0; i < 1000; i++) {
            String k = Long.toHexString(rand.nextLong());
            DBValue ovm = map.put(k, new DBValue(k));
            DBValue ovs = store.getPutValue(k, new DBValue(k));
            Assert.assertEquals("i=" + i, ovm.getVal(), ovs.getVal());
        }
        // all range test
        if (log.isDebugEnabled()) log.debug("range compare");
        rangeCompare("all", map.entrySet().iterator(), store.range(null, true));
        // random range test
        if (log.isDebugEnabled()) log.debug("random range compare");
        for (int i = 0; i < 1000; i++) {
            String k = Long.toHexString(rand.nextLong());
            Iterator<Entry<String, DBValue>> mi = map.tailMap(k).entrySet().iterator();
            Iterator<Entry<String, DBValue>> si = store.range(k, true);
            rangeCompare("i=" + i + ",k=" + k, mi, si);
        }
    }

    @Test
    public void externalPagedStore1() {
        File temp = new File("test.bdb.temp");
        // test 1
        ExternalPagedStore<String, DBValue> eps = new ExternalPagedStore<>(new SimpleStringKeyCoder(),
                new ByteStoreBDB(temp, "test", false), 3, 3);
        insertIterateTest(eps);
        eps.close();
        // test 2
        eps = new ExternalPagedStore<>(new SimpleStringKeyCoder(), new ByteStoreBDB(temp, "test", false), 3, 3);
        iterateTest(eps);
        eps.close();
        Files.deleteDir(temp);
    }

    @Test
    public void externalPagedStore2() {
        File temp = new File("test.bdb.temp");
        ExternalPagedStore<String, DBValue> eps = new ExternalPagedStore<>(new SimpleStringKeyCoder(),
                new ByteStoreBDB(temp, "test", false), 3, 3);
        insertDeleteOddIterateTest(eps);
        eps.close();
        Files.deleteDir(temp);
    }

    @Test
    public void externalPagedStore3() {
        File temp = new File("test.bdb.temp");
        ExternalPagedStore<String, DBValue> eps = new ExternalPagedStore<>(new SimpleStringKeyCoder(),
                new ByteStoreBDB(temp, "test", false), 3, 3);
        randomInsertRangeRead(eps);
        eps.close();
        Files.deleteDir(temp);
    }

    @After
    public void cleanup() {
        Files.deleteDir(new File("test.bdb.temp"));
    }
}

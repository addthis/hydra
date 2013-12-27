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
package com.addthis.hydra.store.db;

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import java.text.DecimalFormat;

import com.addthis.basis.util.Files;

import com.addthis.hydra.store.db.IPageDB.Range;
import com.addthis.hydra.store.kv.CachedPagedStore;
import com.addthis.hydra.store.kv.ExternalPagedStore;
import com.addthis.hydra.store.util.Raw;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPagedDB {

    private static final Logger log = LoggerFactory.getLogger(TestPagedDB.class);

    private static final boolean repeattest = System.getProperty("test.pagedb.repeat", "1") != "0";
    private static final boolean verbose = System.getProperty("test.pagedb.verbose", "0") != "0";
    private static final DecimalFormat dc = new DecimalFormat("000");
    private static final int pagesize = 7;
    private static final int pagecache = 3;
    protected static int keyValueStoreType = 0;

    @BeforeClass
    public static void oneTimeSetup() {
        keyValueStoreType = 0;
        CachedPagedStore.setDebugLocks(true);
        ExternalPagedStore.setKeyDebugging(true);
    }

    @After
    public void cleanupTestDirs() throws Exception {
        System.out.println("cleaning up " + testDirs.size() + " test directories " + getClass());
        for (File dir : testDirs) {
            Files.deleteDir(dir);
        }
    }

    @Test
    public void emptyTest() {
        // Because
    }

    private final LinkedList<File> testDirs = new LinkedList<>();
    private final HashMap<IPageDB<DBKey, Raw>, File> dbToDir = new HashMap<>();

    protected enum DB {PageDB, DirectDB}

    /*
     * returns a key in the format jjj:iii where j is a rounded up 10x of i.
     * where i = 85, j = 900
     * where i = 62, j = 700
     * where i = 14, j = 200
     * where i =  5, j = 100
     */
    private static DBKey tidyKey(int i) {
        int j = i + 10;
        return new DBKey((j * 10) - ((j * 10) % 100), tidyValue(i));
    }

    private static Raw tidyValue(int i) {
        return Raw.get(dc.format(i));
    }

    private static ArrayList<Raw> listToRaw(int v[]) {
        ArrayList<Raw> al = new ArrayList<>(v.length);
        for (int i : v) {
            al.add(tidyValue(i));
        }
        return al;
    }

    protected IPageDB<DBKey, Raw> createDB(DB type) throws Exception {
        File parentDir = Files.createTempDir();
        File dir = new File(parentDir + "/testdb." + type + "." + System.currentTimeMillis());
        Files.deleteDir(parentDir);
        testDirs.add(parentDir);
        return openDB(dir, type);
    }

    private IPageDB<DBKey, Raw> openDB(File dir, DB type) throws Exception {
        if (dir != null) {
            dir.deleteOnExit();
            testDirs.add(dir);
        }
        IPageDB<DBKey, Raw> db = null;
        switch (type) {
            case PageDB:
                db = new PageDB<>(dir, Raw.class, PageDB.defaultDbName,
                        pagesize, pagecache, keyValueStoreType, false);
                break;
        }
        dbToDir.put(db, dir);
        return db;
    }

    private IPageDB<DBKey, Raw> reopen(IPageDB<DBKey, Raw> db, DB type) throws Exception {
        return openDB(dbToDir.get(db), type);
    }

    protected void reopenChangeType(DB type1, DB type2) throws Exception {
        IPageDB<DBKey, Raw> pt = createDB(type1);
        fromTopAndBottom(true, true, pt, true, false);
        fromTopAndBottom(true, false, pt, true, false);
        fromTopAndBottom(false, true, pt, true, false);
        fromTopAndBottom(true, true, pt, false, true);
        IPageDB<DBKey, Raw> rept = reopen(pt, type2);
        checkRange(rept, tidyKey(0), tidyKey(200), 10);
        checkRange(rept, tidyKey(0), tidyKey(200));
        checkRange(rept, new DBKey(400), new DBKey(601));
        checkRange(rept, tidyKey(40), tidyKey(60));
        rept.close();
    }

    public void reopenTest(DB type) throws Exception {
        final AtomicInteger failCount = new AtomicInteger(0);
        final int testCount = 20;
        for (int i = 0; i < testCount; i++) {
            IPageDB<DBKey, Raw> pt = createDB(type);
            if (verbose) log.warn("ReOpen ITER=" + i + " DB=" + pt);
            fromTopAndBottom(true, true, pt, true, false);
            fromTopAndBottom(true, false, pt, true, false);
            fromTopAndBottom(false, true, pt, true, false);
            if (repeattest) {
                fromTopAndBottom(true, true, pt, false, true);
                final IPageDB<DBKey, Raw> rept = reopen(pt, type);
                // run in a separate thread to test real-world force range closure
                Thread t = new Thread("*range thread*") {
                    public void run() {
                        try {
                            // designed to leave an open range
                            checkRange(rept, tidyKey(0), tidyKey(200), 10);
                            // these ranges close properly, but the first will have to get
                            // past an unclosed/held lock from the previous range test
                            checkRange(rept, tidyKey(0), tidyKey(200));
                            checkRange(rept, new DBKey(400), new DBKey(601));
                            checkRange(rept, tidyKey(40), tidyKey(60));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            failCount.incrementAndGet();
                        }
                    }
                };
                t.start();
                t.join();
                rept.close();
            }
        }
        assertEquals(failCount.get(), 0);
    }

    private void repeatReopenX1000(DB type) throws Exception {
        for (int i = 0; i < 1000; i++) {
            reopenTest(type);
        }
    }

    protected void sizeTests(DB type) throws Exception {
        IPageDB<DBKey, Raw> pt = createDB(type);
        pt.setCacheSize(3);
        pt.setPageSize(1000);
        pt.setCacheMem(10 * 1024 * 1024); // 10MB
        pt.setPageMem(150 * 1024); // 150k
        pt.setMemSampleInterval(1);

        Raw chunk1k = Raw.get(new byte[1024]);
        for (int i = 0; i < 1000; i++) {
            pt.put(tidyKey(i), chunk1k);
        }
        Thread.sleep(1000);
        printDBStats(pt);

        Raw chunk2k = Raw.get(new byte[2048]);
        for (int i = 0; i < 1000; i++) {
            pt.put(tidyKey(i), chunk2k);
        }
        Thread.sleep(2000);
        printDBStats(pt);

        Raw chunk4k = Raw.get(new byte[4096]);
        for (int i = 0; i < 1000; i++) {
            pt.put(tidyKey(i), chunk4k);
        }
        Thread.sleep(3000);
        printDBStats(pt);

        Raw chunk8k = Raw.get(new byte[8192]);
        for (int i = 0; i < 1000; i++) {
            pt.put(tidyKey(i), chunk8k);
        }
        Thread.sleep(4000);
        printDBStats(pt);

        pt.close();
    }

    // @Test
    protected void oomTest(DB type) throws Exception {
        oomTest(false, type);
        oomTest(true, type);
    }

    private void oomTest(boolean reopen, DB type) throws Exception {
        IPageDB<DBKey, Raw> pt = createDB(type);
        pt.setCacheSize(3);
        pt.setPageSize(1000);
        pt.setCacheMem(10 * 1024 * 1024); // 10MB
        pt.setPageMem(150 * 1024); // 150k
        pt.setMemSampleInterval(1);

        Raw chunk1k = Raw.get(new byte[1024 * 1024]);
        long mark = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            pt.put(tidyKey(i), chunk1k);
        }
        System.out.println((System.currentTimeMillis() - mark) + " ms to insert 100GB of 1MB records");
        Thread.sleep(4000);
        printDBStats(pt);

        if (reopen) {
            pt.close();
            pt = reopen(pt, type);
            pt.setCacheSize(3);
            pt.setPageSize(1000);
            pt.setCacheMem(10 * 1024 * 1024); // 10MB
            pt.setPageMem(150 * 1024); // 150k
            pt.setMemSampleInterval(1);
        }

        Range<DBKey, Raw> r = pt.range(new DBKey(0, Raw.get(new byte[0])), null);
        int count = 0;
        mark = System.currentTimeMillis();
        while (r.hasNext()) {
            Entry<DBKey, Raw> next = r.next();
            if (++count % 1000 == 0) {
                long delta = System.currentTimeMillis() - mark;
                mark += delta;
                System.out.println(count + "] r.next = " + next.getKey() + " @ " + delta);
            }
        }
        r.close();

        pt.close();
    }

    private void printDBStats(IPageDB<DBKey, Raw> ipdb) {
        System.out.println("print db stats : " + ipdb.toString());
    }

    private void checkRange(IPageDB<DBKey, Raw> pt, DBKey from, DBKey to) {
        checkRange(pt, from, to, null);
    }

    private void checkRange(IPageDB<DBKey, Raw> pt, DBKey from, DBKey to, List<Raw> require) {
        checkRange(pt, from, to, -1, require);
    }

    private void checkRange(IPageDB<DBKey, Raw> pt, DBKey from, DBKey to, int iter) {
        checkRange(pt, from, to, iter, null);
    }

    private void checkRange(IPageDB<DBKey, Raw> pt, DBKey from, DBKey to, int iter, List<Raw> require) {
        if (verbose) {
            System.out.println("RANGE " + from + " -- " + to + " in thread " + Thread.currentThread());
        }
        IPageDB.Range<DBKey, Raw> range = pt.range(from, to);
        List<Raw> missed = new LinkedList<>();
        List<Raw> removed = new LinkedList<>();
        for (Entry<DBKey, Raw> e : range) {
            if (require != null) {
                boolean ok = require.remove(e.getValue());
                if (!ok) {
                    missed.add(e.getValue());
                } else {
                    removed.add(e.getValue());
                }
            }
            if (verbose) {
                System.out.println(e.getKey() + " = " + e.getValue());
            }
            if (--iter == 0) {
                if (verbose) {
                    System.out.println("exit on iter decrement " + range);
                }
                break;
            }
        }
        if (verbose) {
            System.out.println("range not removed " + missed);
            System.out.println("range removed " + removed);
        }
        assertTrue("remaining=" + require, require == null || require.isEmpty());
    }

    /**
     * sequential insert test
     */
    protected void fromTop(IPageDB<DBKey, Raw> pt) {
        fromTopAndBottom(true, false, pt, true, true);
    }

    /**
     * sequential insert test
     */
    protected void fromBottom(IPageDB<DBKey, Raw> pt) {
        fromTopAndBottom(false, true, pt, true, true);
    }

    /**
     * defaults to delete=true and close=true
     */
    protected void fromTopAndBottom(IPageDB<DBKey, Raw> pt) {
        fromTopAndBottom(true, true, pt, true, true);
    }

    /**
     * Iterates over a range of keys and inserts sequentially low to high, high to low or both interleaved.
     * <p/>
     * optionally deletes the data after the test
     * optionally closes the database after the test
     */
    protected void fromTopAndBottom(boolean top, boolean bottom, IPageDB<DBKey, Raw> pt, boolean delete, boolean close) {
        // PUT (000:000 to 600:058) and/or (1100:101 to 500:043)
        for (int i = 0; i < 60; i += 2) {
            /*
			 *  inserts from the top (0-60 * 2) in even increments up to 60 using tidyKey
			 *  100:000
			 *  100:002
			 *  100:004, etc
			 */
            if (top) {
                DBKey k = tidyKey(i);
                if (verbose) log.warn("put.top " + k);
                assertTrue("PUT " + k, testEquals(pt.put(k, k.rawKey()), null));
            }
			/*
			 *  inserts from the bottom (101-41 / 2) in odd increments down to 41 using tidyKey
			 *  100:000
			 *  100:002
			 *  100:004, etc
			 */
            if (bottom) {
                DBKey k = tidyKey(101 - i);
                if (verbose) log.warn("put.bot " + k);
                assertTrue("PUT " + k, testEquals(pt.put(k, k.rawKey()), null));
            }
        }

        // validate data in the ranges
        if (top) checkRange(pt, tidyKey(0), tidyKey(200), listToRaw(new int[]{0, 16, 32, 48}));
        if (top && bottom) checkRange(pt, new DBKey(400), new DBKey(601), listToRaw(new int[]{32, 48, 43, 53}));
        if (bottom) checkRange(pt, tidyKey(40), tidyKey(60), listToRaw(new int[]{43, 53, 59}));

        // GET (validate data)
        for (int i = 0; i < 60; i += 2) {
            if (top) {
                DBKey k = tidyKey(i);
                if (verbose) log.warn("get.top i=" + i + " " + k);
                assertTrue("GET " + k, testEquals(pt.get(k), k.rawKey()));
            }
            if (bottom) {
                DBKey k = tidyKey(101 - i);
                if (verbose) log.warn("get.bot i=" + i + " " + k);
                assertTrue("GET " + k, testEquals(pt.get(k), k.rawKey()));
            }
        }

        // DELETE if requested, and re-validate key/value
        if (delete) {
            for (int i = 0; i < 60; i += 2) {
                if (top) {
                    DBKey k = tidyKey(i);
                    assertTrue("DEL " + k, testEquals(pt.remove(k), k.rawKey()));
                }
                if (bottom) {
                    DBKey k = tidyKey(101 - i);
                    assertTrue("DEL " + k, testEquals(pt.remove(k), k.rawKey()));
                }
            }
        }

        if (close) {
            pt.close();
        }
    }

    private boolean testEquals(Object o1, Object o2) {
        try {
            return o1 == o2 || o1.equals(o2);
        } catch (RuntimeException ex) {
            System.out.println("FAIL on " + o1 + " == " + o2);
            throw ex;
        }
    }
}

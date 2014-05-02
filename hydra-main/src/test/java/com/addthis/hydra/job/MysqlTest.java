package com.addthis.hydra.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.job.store.MysqlDataStore;
import com.addthis.hydra.job.store.MysqlInsertPickLastDataStore;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MysqlTest {

    private static final String user = "spawn";
    private static final String password = "pw";


    @Test
    public void localTest() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        String tableName = "newTable2";
        MysqlInsertPickLastDataStore ds = new MysqlInsertPickLastDataStore("jdbc:mysql:thin://localhost:3306/test", tableName, properties);
        correctnessTestDataStore(ds);
        perfTest(ds);
        ds.close();
    }

    @Test
    public void concurTest() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        String tableName = "newTable2";
        MysqlInsertPickLastDataStore ds = new MysqlInsertPickLastDataStore("jdbc:mysql:thin://localhost:3306/test", tableName, properties);
        ExecutorService exec = new ScheduledThreadPoolExecutor(40, new ThreadFactoryBuilder().build());
        for (int i=0; i<100; i++) {
            exec.submit(new DbRunner(ds, i/10));
        }
        exec.shutdown();
        exec.awaitTermination(500, TimeUnit.SECONDS);
        ds.close();
    }
    private class DbRunner implements Runnable{
        private final SpawnDataStore ds;
        private final int id;

        private DbRunner(SpawnDataStore ds, int id) {
            this.ds = ds;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                String key = "k" + id;
                String val = "v" + id;
                long wait = Math.round(5000 * Math.random());
                Thread.sleep(wait);
                ds.put(key, val);
                ds.putAsChild(key, "child1", val);
                Thread.sleep(wait);
                boolean passed = val.equals(ds.get(key)) && val.equals(ds.getChild(key, "child1"));
                System.out.println(id + " " + (passed ? "passed" : "failed"));
            } catch (Exception e) {
                System.out.println("ERROR IN DBRUNNER: " + e);
                e.printStackTrace();
            }
        }
    }

    private static String bigJson = "";


    @Test
    public void remotePerfTest() throws Exception {
        JSONObject obj = new JSONObject();
        for (int i=0; i<1000; i++) {
            obj.put("k" + i, "v" + i);
        }
        bigJson = obj.toString();
        Properties properties = new Properties();
        properties.setProperty("user", "spawn");
        properties.setProperty("password", "rPmuq3sPfK9Ob0BY");
        MysqlDataStore ds1 = new MysqlDataStore("ldm15f", 3306, "spawndatastore", "perftest2", properties);
        MysqlInsertPickLastDataStore ds2 = new MysqlInsertPickLastDataStore("jdbc:mysql:thin://ldm15f:3306/spawndatastore", "test", properties);
        for (int i=0; i<4; i++) {
            perfTest(ds1);
            perfTest(ds2);
        }
        ds1.close();
        ds2.close();
    }

    private void perfTest(SpawnDataStore ds) throws Exception {
        long start = System.currentTimeMillis();
        ds.put("a", "old");
        ds.put("a", "new");
        ds.put("c", bigJson);
        ds.putAsChild("p", "b", "old");
        ds.putAsChild("p", "b", "new");
        ds.putAsChild("p", "c", "new2");
        System.out.println(ds.getDescription() + " write time: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        assertEquals("new", ds.get("a"));
        assertNull(ds.get("b"));
        assertEquals(ImmutableSet.of("b", "c"), ImmutableSet.copyOf(ds.getChildrenNames("p")));
        assertEquals(ImmutableMap.of("b", "new", "c", "new2"), ds.getAllChildren("p"));
        System.out.println(ds.getDescription() + " read time: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        ds.delete("a");
        ds.delete("c");
        ds.deleteChild("p", "b");
        ds.deleteChild("p", "c");
        System.out.println(ds.getDescription() + " delete time: " + (System.currentTimeMillis()-start));
    }

    private void correctnessTestDataStore(SpawnDataStore spawnDataStore) throws Exception {
        String key1 = "key1";
        String val1 = "value1";
        String key2 = "key2";
        String val2 = "value!!\"{ !!'' }[,'],;';;'\n_\\";

        spawnDataStore.put(key1, "old1");
        spawnDataStore.delete(key1);
        spawnDataStore.put(key1, "old2");
        spawnDataStore.put(key1, val1);
        spawnDataStore.put(key2, val2);
        assertNull("should get null for non-inserted key", spawnDataStore.get("key5"));
        assertEquals("should get latest value", val1, spawnDataStore.get(key1));
        assertEquals("should correctly fetch value with extra characters", val2, spawnDataStore.get(key2));
        Map<String, String> expected = ImmutableMap.of(key1, val1, key2, val2);

        String nullKey = "nullkey";
        spawnDataStore.put(nullKey, "val");
        spawnDataStore.put(nullKey, null);
        assertNull("should get null for key inserted as null", spawnDataStore.get(nullKey));

        assertEquals("should get expected map from multi-fetch call", expected, spawnDataStore.get(new String[]{key1, key2, "otherKey", "other'Key\nwithWeird;;';Characters"}));
        spawnDataStore.putAsChild("parent", "child1", val1);
        spawnDataStore.putAsChild("parent", "child2", "val2");
        spawnDataStore.deleteChild("parent", "child2");
        spawnDataStore.putAsChild("parent", "child3", val2);
        assertEquals("should get expected child value", spawnDataStore.getChild("parent", "child1"), val1);
        spawnDataStore.put("parent", "parentvalue");
        List<String> expectedChildren = ImmutableList.of("child1", "child3");
        assertEquals("should get expected children list", expectedChildren, spawnDataStore.getChildrenNames("parent"));
        assertEquals("should get correct parent value", "parentvalue", spawnDataStore.get("parent"));
        Map<String, String> expectedChildrenMap = ImmutableMap.of("child1", val1, "child3", val2);
        assertEquals("should get expected children map", expectedChildrenMap, spawnDataStore.getAllChildren("parent"));
        assertEquals("should get empty list for non-existent parent", new ArrayList<String>(), spawnDataStore.getChildrenNames("PARENT_NO_EXIST"));
        assertEquals("should get empty map for non-existent parent", new HashMap<String, String>(), spawnDataStore.getAllChildren("PARENT_NO_EXIST"));
    }

}

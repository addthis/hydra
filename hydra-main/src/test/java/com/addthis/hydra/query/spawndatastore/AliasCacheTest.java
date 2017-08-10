package com.addthis.hydra.query.spawndatastore;

import com.addthis.hydra.job.alias.AliasManagerImpl;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AliasCacheTest {
    private static final String ALIAS_PATH = "/query/alias";

    @Test
    public void testPropagationFromAliasCache() throws Exception {
        // 1. AliasManagerImpl
        AliasManagerImpl abm = new AliasManagerImpl();
        abm.putAlias("a1", ImmutableList.of("j1", "j2"));

        AliasManagerImpl r_abm = new AliasManagerImpl();
        assertEquals(ImmutableList.of("j1", "j2"), r_abm.getJobs("a1"));
//        assertEquals("a1", r_abm.getLikelyAlias("j1"));

        // 2. AliasCache
        AliasCache ac = new AliasCache();
        SpawnDataStore spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
        System.out.println("udpate alis =  " + ac.getJobsFromDatastore("a1", spawnDataStore.getChild(ALIAS_PATH, "a1")));
    }

    @Test
    public void testGet() throws Exception {
        AliasManagerImpl abm = new AliasManagerImpl();
        abm.putAlias("test_alias1", ImmutableList.of("tJob1", "tJob2"));

        AliasManagerImpl r_abm = new AliasManagerImpl();
        assertEquals(ImmutableList.of("tJob1", "tJob2"), r_abm.getJobs("test_alias1"));

        abm.putAlias("test_alias1", ImmutableList.of("tJob11", "tJob12"));
        r_abm.putAlias("test_alias1", ImmutableList.of("tJob31", "tJob32"));

        // 2. AliasCache
        AliasCache ac = new AliasCache();
        SpawnDataStore spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
        System.out.println("get jobs for test_alias1 =  " + ac.getJobs("test_alias1"));
    }

    @Test
    public void testPropagation() throws Exception {
        AliasCache ac = new AliasCache();
        SpawnDataStore spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();


        AliasManagerImpl abm = new AliasManagerImpl(spawnDataStore);
        abm.putAlias("A1", ImmutableList.of("J1", "J2"));

        AliasManagerImpl r_abm = new AliasManagerImpl(spawnDataStore);
        assertEquals(ImmutableList.of("J1", "J2"), ac.getJobs("A1"));

        abm.putAlias("a2", ImmutableList.of("j21"));
        Thread.sleep(350);

        assertEquals(ImmutableList.of("j21"), ac.getJobs("a2"));

        abm.putAlias("a1", ImmutableList.of("j7", "j8"));
        Thread.sleep(350);
        assertEquals(ImmutableList.of("j7", "j8"), ac.getJobs("a1"));

        abm.deleteAlias("a1");
//        ac.deleteAlias("a1");
        Thread.sleep(2000);

        assertNull(ac.getJobs("a1"));
    }

    @Test
    public void testRemove() throws Exception {
        AliasCache ac = new AliasCache();

        AliasManagerImpl abm = new AliasManagerImpl();
        abm.putAlias("a1", ImmutableList.of("j7", "j8"));
        Thread.sleep(350);
        assertEquals(ImmutableList.of("j7", "j8"), ac.getJobs("a1"));

        abm.deleteAlias("a1");
//        ac.deleteAlias("a1");
        Thread.sleep(3000);

        assertNull(ac.getJobs("a1"));
    }

    @Test
    public void updateLoop() throws Exception {
        AliasCache ac = new AliasCache();
        AliasManagerImpl abm = new AliasManagerImpl();
        AliasManagerImpl r_abm = new AliasManagerImpl();

        for (int i = 0; i < 5; i++) {
            int retries = 10;
            boolean succeeded = false;
            String is = Integer.toString(i);

            abm.putAlias("a1", ImmutableList.of(is));
            r_abm.putAlias("a1", ImmutableList.of(is));
            while (retries-- > 0) {
                if (ImmutableList.of(is).equals(ac.getJobs("a1"))) {
                    succeeded = true;
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue("failed to register updates after retrying", succeeded);
        }
    }


}


package com.addthis.hydra.query.spawndatastore;

import com.addthis.hydra.job.alias.AliasManagerImpl;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpawnDataStoreHandlerTest {
    private AliasCache ac;
    private AliasManagerImpl abm;

    @Before
    public void setUp() throws Exception {
        ac = new AliasCache();
        abm = new AliasManagerImpl();
        abm.putAlias("a1", ImmutableList.of("j11", "j12"));
        Thread.sleep(350);
    }

    @Test
    public void testExpandAlias() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        assertEquals(ImmutableList.of("j11", "j12"), spawnDataStoreHandler.expandAlias("a1"));
    }

    @Test
    public void testResolveAlias() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        assertEquals("j11", spawnDataStoreHandler.resolveAlias("a1"));
    }
}
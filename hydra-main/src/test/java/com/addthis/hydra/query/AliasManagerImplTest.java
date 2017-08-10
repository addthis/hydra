package com.addthis.hydra.query;

import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.alias.AliasManagerImpl;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AliasManagerImplTest {
    @Test
    public void testConstruction() throws Exception {
        AliasManager abm = new AliasManagerImpl();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testInit() throws Exception {
        AliasManagerImpl abm = new AliasManagerImpl();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testInit2() throws Exception {
        AliasManagerImpl abm = new com.addthis.hydra.job.alias.AliasManagerImpl();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testLocalGetPut() throws Exception {
        AliasManagerImpl abm = new com.addthis.hydra.job.alias.AliasManagerImpl();
        assertNull(abm.getJobs("foo"));
        abm.putAlias("a1", ImmutableList.of("j1", "j2"));
        assertEquals(ImmutableList.of("j1", "j2"), abm.getJobs("a1"));
        assertEquals("a1", abm.getLikelyAlias("j1"));

        abm.deleteAlias("a1");
        assertNull(abm.getJobs("a1"));
        assertNull(abm.getLikelyAlias("j1"));
    }

    @Test
    public void testPropagation() throws Exception {
//        System.setProperty("alias.bimap.expire", "50");
        AliasManagerImpl abm = new com.addthis.hydra.job.alias.AliasManagerImpl();
        abm.putAlias("a1", ImmutableList.of("j1", "j2"));

        AliasManagerImpl r_abm = new com.addthis.hydra.job.alias.AliasManagerImpl();
        assertEquals(ImmutableList.of("j1", "j2"), r_abm.getJobs("a1"));
        assertEquals("a1", r_abm.getLikelyAlias("j1"));

        abm.putAlias("a2", ImmutableList.of("j21"));
        Thread.sleep(350);
        assertEquals(ImmutableList.of("j21"), r_abm.getJobs("a2"));
        assertEquals("a2", r_abm.getLikelyAlias("j21"));

        abm.putAlias("a1", ImmutableList.of("j7", "j8"));
        Thread.sleep(350);
        assertEquals(ImmutableList.of("j7", "j8"), r_abm.getJobs("a1"));
        assertEquals("a1", r_abm.getLikelyAlias("j7"));

        abm.deleteAlias("a1");
        Thread.sleep(350);
        assertNull(r_abm.getJobs("a1"));
        assertNull(r_abm.getLikelyAlias("j1"));
    }

    @Test
    public void updateLoop() throws Exception {
        System.setProperty("alias.bimap.refresh", "500");
        AliasManagerImpl abm = new com.addthis.hydra.job.alias.AliasManagerImpl();
        AliasManagerImpl r_abm = new com.addthis.hydra.job.alias.AliasManagerImpl();

        for (int i = 0; i < 5; i++) {
            int retries = 10;
            boolean succeeded = false;
            String is = Integer.toString(i);
            abm.putAlias("a1", ImmutableList.of(is));
            while (retries-- > 0) {
                if (ImmutableList.of(is).equals(r_abm.getJobs("a1"))) {
                    succeeded = true;
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue("failed to register updates after retrying", succeeded);
        }
    }
}

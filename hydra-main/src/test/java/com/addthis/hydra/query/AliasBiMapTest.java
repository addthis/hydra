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
package com.addthis.hydra.query;

import com.addthis.basis.test.SlowTest;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.query.spawndatastore.AliasBiMap;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
public class AliasBiMapTest extends ZkStartUtil {

    @Test
    public void testConstruction() throws Exception {
        AliasBiMap abm = new AliasBiMap();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testInit() throws Exception {
        AliasBiMap abm = new AliasBiMap();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testInit2() throws Exception {
        AliasBiMap abm = new AliasBiMap();
        assertNull(abm.getJobs("foo"));
        assertNull(abm.getLikelyAlias("foo"));
    }

    @Test
    public void testLocalGetPut() throws Exception {
        AliasBiMap abm = new AliasBiMap();
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
        System.setProperty("alias.bimap.expire", "50");
        AliasBiMap abm = new AliasBiMap();
        abm.putAlias("a1", ImmutableList.of("j1", "j2"));

        AliasBiMap r_abm = new AliasBiMap();
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
        AliasBiMap abm = new AliasBiMap();

        AliasBiMap r_abm = new AliasBiMap();

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
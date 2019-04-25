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
package com.addthis.hydra.query.spawndatastore;

import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.alias.AliasManagerImpl;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AliasCacheTest {
    @Test
    public void testGetJobs() throws Exception {
        AliasManager abm1 = new AliasManagerImpl();
        abm1.putAlias("a1", ImmutableList.of("j11", "j12"));

        AliasManager abm2 = new AliasManagerImpl();
        abm2.putAlias("a2", ImmutableList.of("j21", "j22"));

        AliasCache ac = new AliasCache();
        assertEquals(ImmutableList.of("j11", "j12"), ac.getJobs("a1"));
        assertEquals(ImmutableList.of("j21", "j22"), ac.getJobs("a2"));
    }

    @Test
    public void testGetJob_Update() throws Exception {
        AliasManager abm1 = new AliasManagerImpl();
        abm1.putAlias("a1", ImmutableList.of("j11", "j12"));

        AliasManager abm2 = new AliasManagerImpl();
        abm2.putAlias("a2", ImmutableList.of("j21", "j22"));

        AliasCache ac = new AliasCache();

        abm1.putAlias("a1", ImmutableList.of("j110", "j120"));
        Thread.sleep(3000);
        assertEquals(ImmutableList.of("j110", "j120"), ac.getJobs("a1"));
    }

    @Test
    public void testGetJob_Remove() throws Exception {
        AliasManager abm = new AliasManagerImpl();
        abm.putAlias("a1", ImmutableList.of("j1", "j2"));
        Thread.sleep(350);

        AliasCache ac = new AliasCache();
        assertEquals(ImmutableList.of("j1", "j2"), ac.getJobs("a1"));

        abm.deleteAlias("a1");
        Thread.sleep(3000);
        assertNull(ac.getJobs("a1"));
    }

    @Test
    public void testGetJob_Loop() throws Exception {
        AliasCache ac = new AliasCache();
        AliasManager abm1 = new AliasManagerImpl();

        for (int i = 0; i < 5; i++) {
            int retries = 10;
            boolean succeeded = false;
            String is = Integer.toString(i);
            abm1.putAlias("a1", ImmutableList.of(is));
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

    @After
    public void cleanUp() throws Exception {
        AliasManager abm1 = new AliasManagerImpl();
        abm1.deleteAlias("a1");
        abm1.deleteAlias("a2");
    }
}


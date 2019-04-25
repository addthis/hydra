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

import com.addthis.hydra.job.alias.AliasManagerImpl;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpawnDataStoreHandlerTest {
    private AliasCache ac;
    private AliasManagerImpl abm1;
    private AliasManagerImpl abm2;

    @Before
    public void setUp() throws Exception {
        abm1 = new AliasManagerImpl();
        abm2 = new AliasManagerImpl();

        abm1.putAlias("a1", ImmutableList.of("j11", "j12"));
        abm2.putAlias("a2", ImmutableList.of("j21", "j22"));
        Thread.sleep(350);
    }

    @Test
    public void testExpandAlias() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        assertEquals(ImmutableList.of("j11", "j12"), spawnDataStoreHandler.expandAlias("a1"));
        assertEquals(ImmutableList.of("j21", "j22"), spawnDataStoreHandler.expandAlias("a2"));
    }

    @Test
    public void testResolveAlias() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        assertEquals("j11", spawnDataStoreHandler.resolveAlias("a1"));
    }

    @Test
    public void testExpandAlias_Update() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        abm1.putAlias("a1", ImmutableList.of("j110", "j120"));
        Thread.sleep(3000);
        assertEquals(ImmutableList.of("j110", "j120"), spawnDataStoreHandler.expandAlias("a1"));
    }

    @Test
    public void testResolveAlias_Update() throws Exception {
        SpawnDataStoreHandler spawnDataStoreHandler = new SpawnDataStoreHandler();
        abm1.putAlias("a1", ImmutableList.of("j110", "j120"));
        Thread.sleep(3000);
        assertEquals("j110", spawnDataStoreHandler.resolveAlias("a1"));
    }

    @After
    public void cleanUp() throws Exception {
        abm1.deleteAlias("a1");
        abm1.deleteAlias("a2");
    }
}
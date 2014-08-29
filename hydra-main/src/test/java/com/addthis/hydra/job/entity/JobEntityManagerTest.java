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
package com.addthis.hydra.job.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.spawn.Spawn;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.collections.Sets;

public class JobEntityManagerTest extends JobEntityManagerTestBase {

    private static final String DATA_STORE_PATH = "/spawn/common/myentity";

    private MyJobEntity e1;
    private MyJobEntity e2;
    private MyJobEntityManager manager;

    @Before
    public void setUp() throws Exception {
        initSpawnMocks();
        e1 = new MyJobEntity("entity #1");
        e2 = new MyJobEntity("entity #2");
    }

    @Test
    public void initialLoad() throws Exception {
        // stub data loaded from data store on initialization
        Map<String, String> dataStoreMap = new HashMap<>();
        dataStoreMap.put("e1", CodecJSON.encodeString(e1));
        dataStoreMap.put("e2", CodecJSON.encodeString(e2));
        when(spawnDataStore.getAllChildren(DATA_STORE_PATH)).thenReturn(dataStoreMap);

        manager = new MyJobEntityManager(spawn);
        assertEquals(2, manager.size());
    }

    @Test
    public void initialLoadNoMacros() throws Exception {
        new MyJobEntityManager(spawn);
        verify(spawnDataStore).getAllChildren(DATA_STORE_PATH);
    }

    @Test
    public void createAndStore() throws Exception {
        manager = new MyJobEntityManager(spawn);
        manager.putEntity("e1", e1, true);

        assertEquals("size after put", 1, manager.size());

        // verify entity json passed as an argument to the data store save method 
        ArgumentCaptor<String> arg = ArgumentCaptor.forClass(String.class);
        verify(spawnDataStore).putAsChild(eq(DATA_STORE_PATH), eq("e1"), arg.capture());
        MyJobEntity m = CodecJSON.decodeString(MyJobEntity.class, arg.getValue());
        assertEquals("entity name", e1.name, m.name);
    }

    @Test
    public void updateAndNotStore() throws Exception {
        manager = new MyJobEntityManager(spawn);
        manager.putEntity("e1", e1, false);
        manager.putEntity("e1", e2, false);

        assertEquals("size after put", 1, manager.size());
        assertEquals("get macro", e2, manager.getEntity("e1"));
        verify(spawnDataStore, never()).putAsChild(anyString(), anyString(), anyString());
    }

    @Test
    public void getKeys() throws Exception {
        manager = new MyJobEntityManager(spawn);
        manager.putEntity("e1", e1, false);
        manager.putEntity("e2", e2, false);
        assertEquals(Sets.newSet("e1", "e2"), manager.getKeys());
    }

    @Test
    public void delete() throws Exception {
        manager = new MyJobEntityManager(spawn);
        manager.putEntity("e1", e1, false);
        manager.putEntity("e2", e2, false);
        assertTrue("deleted", manager.deleteEntity("e1"));
        assertEquals("size", 1, manager.size());
        assertNull("no longer exists", manager.getEntity("e1"));
        verify(spawnDataStore).deleteChild(DATA_STORE_PATH, "e1");
    }

    @Test
    public void deleteUsed() throws Exception {
        // mock a job that uses the macro 
        manager = new MyJobEntityManager(spawn) {
            @Override
            protected Job findDependentJob(Spawn spawn, String entityKey) {
                return new Job("my_mock_job", "bob");
            }
        };
        manager.putEntity("e1", e1, false);
        manager.putEntity("e2", e2, false);
        assertFalse("did not delete", manager.deleteEntity("e1"));
        assertEquals("size", 2, manager.size());
        assertEquals("still exists", e1, manager.getEntity("e1"));
        verify(spawnDataStore, never()).deleteChild(DATA_STORE_PATH, "e1");
    }

    @Test
    public void deleteNonExistent() throws Exception {
        manager = new MyJobEntityManager(spawn);
        manager.putEntity("e2", e2, false);
        assertFalse("did not delete", manager.deleteEntity("e1"));
        assertEquals("size", 1, manager.size());
        verify(spawnDataStore, never()).deleteChild(DATA_STORE_PATH, "e1");
    }

    // ---- classes for this test ------

    public static class MyJobEntity {
        @FieldConfig
        public String name;

        public MyJobEntity() {
        }

        public MyJobEntity(String name) {
            this.name = name;
        }
    }

    public static class MyJobEntityManager extends AbstractJobEntityManager<MyJobEntity> {

        public MyJobEntityManager(Spawn spawn) throws Exception {
            super(spawn, MyJobEntity.class, DATA_STORE_PATH);
        }

        @Override
        protected Job findDependentJob(Spawn spawn, String entityKey) {
            // only a delete test case uses a different return value
            return null;
        }

    }

}

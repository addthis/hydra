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
package com.addthis.hydra.job;

import java.util.Set;

import com.addthis.hydra.job.store.ZookeeperDataStore;
import com.addthis.hydra.util.ZkCodecStartUtil;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SpawnDataStoreTest extends ZkCodecStartUtil {

    ZookeeperDataStore spawnDataStore;

    @Before
    public void createSpawnDataStore() {
        spawnDataStore = new ZookeeperDataStore(zkClient);
    }

    @Test public void simplePutGet() throws Exception {
        String savePath = "/some/path";
        try {
            spawnDataStore.delete(savePath);
            spawnDataStore.put(savePath, "someval");
            assertEquals("should get inserted value", "someval", spawnDataStore.get(savePath));
            spawnDataStore.delete(savePath);
            assertNull("should get null value after deletion", spawnDataStore.get(savePath));
        } finally {
            spawnDataStore.delete(savePath);
        }
    }

    /**
     * Write some children to a specified location. Make sure their respective data is held intact, and that the list
     * of children updates correctly after nodes are added or deleted.
     */
    @Test public void crudChildren() throws Exception {
        String savePath = "/other/path";
        try {
            String c1 = "child1";
            String c2 = "child2";
            spawnDataStore.delete(savePath);
            spawnDataStore.putAsChild(savePath, c1, "data1");
            spawnDataStore.putAsChild(savePath, c1, "data1_updated");
            spawnDataStore.putAsChild(savePath, c2, "data2");
            Set<String> bothChildren = Sets.newHashSet(spawnDataStore.getChildrenNames(savePath));
            assertEquals("should get correct children", bothChildren, Sets.newHashSet(c1, c2));
            assertEquals("should get updated value for c1", "data1_updated", spawnDataStore.getChild(savePath, c1));
            spawnDataStore.deleteChild(savePath, c2);
            Set<String> oneChild = Sets.newHashSet(spawnDataStore.getChildrenNames(savePath));
            assertEquals("should get new correct children", oneChild, Sets.newHashSet(c1));
            assertEquals("c1 should be intact", "data1_updated", spawnDataStore.getChild(savePath, c1));
            spawnDataStore.delete(savePath);
            assertNull("should get no children", spawnDataStore.getChildrenNames(savePath));
        } finally {
            spawnDataStore.delete(savePath);
        }
    }
}

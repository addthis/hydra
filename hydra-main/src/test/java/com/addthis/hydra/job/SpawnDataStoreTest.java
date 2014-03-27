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

import java.util.List;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.ZookeeperDataStore;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SpawnDataStoreTest extends ZkStartUtil {

    /**
     * Run the unit tests using a single SpawnDataStore instance
     */
    @Test
    public void runTests() throws Exception {
        ZookeeperDataStore zookeeperDataStore = new ZookeeperDataStore(zkClient);
        putGetTest(zookeeperDataStore);
        childTest(zookeeperDataStore);
    }

    /**
     * Test simple put/get operations
     *
     * @param spawnDataStore The data store to test
     * @throws Exception
     */
    public void putGetTest(SpawnDataStore spawnDataStore) throws Exception {
        String savePath = "/some/path";
        try {
            spawnDataStore.delete(savePath);
            spawnDataStore.put(savePath, "someval");
            assertEquals("should get inserted value", "someval", spawnDataStore.get(savePath));
            spawnDataStore.delete(savePath);
            assertEquals("should get null value after deletion", null, spawnDataStore.get(savePath));
        } finally {
            spawnDataStore.delete(savePath);
        }
    }

    /**
     * Write some children to a specified location. Make sure their respective data is held intact, and that the list
     * of children updates correctly after nodes are added or deleted.
     *
     * @param spawnDataStore The data store to test
     */
    public void childTest(SpawnDataStore spawnDataStore) throws Exception {
        String savePath = "/other/path";
        try {
            String c1 = "child1";
            String c2 = "child2";
            spawnDataStore.delete(savePath);
            spawnDataStore.putAsChild(savePath, c1, "data1");
            spawnDataStore.putAsChild(savePath, c1, "data1_updated");
            spawnDataStore.putAsChild(savePath, c2, "data2");
            List<String> children = spawnDataStore.getChildrenNames(savePath);
            assertArrayEquals("should get correct children", children.toArray(new String[children.size()]), new String[]{c1, c2});
            assertEquals("should get updated value for c1", spawnDataStore.getChild(savePath, c1), "data1_updated");
            spawnDataStore.deleteChild(savePath, c2);
            children = spawnDataStore.getChildrenNames(savePath);
            assertArrayEquals("should get new correct children", children.toArray(new String[children.size()]), new String[]{c1});
            assertEquals("c1 should be intact", spawnDataStore.getChild(savePath, c1), "data1_updated");
            spawnDataStore.delete(savePath);
            assertEquals("should get no children", null, spawnDataStore.getChildrenNames(savePath));
        } finally {
            spawnDataStore.delete(savePath);
        }
    }
}

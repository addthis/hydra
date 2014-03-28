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
package com.addthis.hydra.job.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.bark.ZkClientFactory;
import com.addthis.bark.ZkHelpers;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * A class for persisting key-value data within zookeeper.
 */
public class ZookeeperDataStore implements SpawnDataStore {

    private final Codec codec = new CodecJSON();
    private static final Logger log = LoggerFactory.getLogger(ZookeeperDataStore.class);
    private static final String description = "zookeeper";

    private final ZkClient zkClient;

    public ZookeeperDataStore(ZkClient zkClient) {
        this.zkClient = zkClient != null ? zkClient : ZkClientFactory.makeStandardClient();
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    /**
     * Read the value from the specified zookeeper path, or return null if it doesn't exist
     */
    public String get(String path) {
        if (ZkHelpers.pathExists(zkClient, path)) {
            String val = ZkHelpers.readDataMaybeNull(zkClient, path);
            return val != null ? val : ""; // If path exists, but has no value, need to return empty String to correctly interpret "marker" nodes
        }
        return null;
    }

    @Override
    public Map<String, String> get(String[] paths) {
        Map<String, String> rv = new HashMap<String, String>();
        for (String path : paths) {
            String val = get(path);
            if (val != null) {
                rv.put(path, val);
            }
        }
        return rv;
    }

    @Override
    /**
     * Put a value into a zookeeper path, after ensuring that it exists
     */
    public void put(String path, String value) throws Exception {
        ZkHelpers.makeSurePersistentPathExists(zkClient, path);
        zkClient.writeData(path, value);
    }

    @Override
    /**
     * Put a child as a parent beneath a parent node. This implementation is trivial in zookeeper, but requires
     * some special handling in PriamDataStore.
     */
    public void putAsChild(String parent, String childId, String value) throws Exception {
        ZkHelpers.makeSurePersistentPathExists(zkClient, parent);
        put(parent + "/" + childId, value);
    }

    @Override
    /**
     * Read data from a zookeeper path and decode it onto a Codable shell object
     */
    public <T extends Codec.Codable> boolean loadCodable(String path, T shell) {
        if (!zkClient.exists(path)) {
            return false;
        }
        String raw = zkClient.readData(path);
        if (raw == null) {
            return false;
        }
        try {
            codec.decode(shell, raw.getBytes());
            return true;
        } catch (Exception e) {
            log.warn("Failed to decode path " + path + ": " + e, e);
            return false;
        }
    }

    @Override
    /**
     * Get the contents of a certain child beneath a parent node. As with putAsChild, this is simple in zookeeper but
     * needs to be handled specially in PriamDataStore.
     */
    public String getChild(String parent, String childId) throws Exception {
        return get(parent + "/" + childId);
    }

    @Override
    /**
     * Delete the key and value of a particular child beneath a parent node
     */
    public void deleteChild(String parent, String childId) {
        ZkHelpers.deletePath(zkClient, parent + "/" + childId);
    }

    @Override
    /**
     * Delete a path from zookeeper
     */
    public void delete(String path) {
        zkClient.deleteRecursive(path);
    }

    @Override
    /**
     * Get all children beneath a zookeeper parent node
     */
    public List<String> getChildrenNames(String path) {
        if (ZkHelpers.pathExists(zkClient, path)) {
            return zkClient.getChildren(path);
        }
        return null;
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        Map<String, String> rv = new HashMap<>();
        List<String> children = getChildrenNames(path);
        if (children != null) {
            for (String child : children) {
                try {
                    rv.put(child, getChild(path, child));
                } catch (Exception ex) {
                    log.warn("Failed to fetch child " + child + " of " + path + ": " + ex, ex);
                    }
            }
        }
        return rv;
    }

    @Override
    /**
     * Close the zkClient if it exists
     */
    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}

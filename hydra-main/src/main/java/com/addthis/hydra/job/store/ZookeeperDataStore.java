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

import com.addthis.bark.StringSerializer;
import com.addthis.bark.ZkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class for persisting key-value data within zookeeper.
 */
public class ZookeeperDataStore implements SpawnDataStore {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperDataStore.class);
    private static final String description = "zookeeper";

    private final CuratorFramework zkClient;

    public ZookeeperDataStore(CuratorFramework zkClient) {
        if (zkClient != null) {
            this.zkClient = zkClient;
        } else {
            this.zkClient = ZkUtil.makeStandardClient();
        }

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
        try {
            if (zkClient.checkExists().forPath(path) != null) {
                String val = StringSerializer.deserialize(zkClient.getData().forPath(path));
                return val != null ? val : ""; // If path exists, but has no value, need to return empty String to correctly interpret "marker" nodes
            }
        } catch (Exception e) {
            log.error("Error getting data for path: " + path, e);
        }
        return null;
    }

    @Override
    public Map<String, String> get(String[] paths) {
        Map<String, String> rv = new HashMap<>();
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
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(path, StringSerializer.serialize(value));
        } catch (KeeperException.NodeExistsException nodeExists) {
            zkClient.setData().forPath(path, StringSerializer.serialize(value));
        }
    }

    @Override
    /**
     * Put a child as a parent beneath a parent node. This implementation is trivial in zookeeper, but requires
     * some special handling in PriamDataStore.
     */
    public void putAsChild(String parent, String childId, String value) throws Exception {
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(parent + "/" + childId, StringSerializer.serialize(value));
        } catch (KeeperException.NodeExistsException nodeExists) {
            zkClient.setData().forPath(parent + "/" + childId, StringSerializer.serialize(value));
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
        try {
            if (zkClient.checkExists().forPath(parent + "/" + childId) != null) {
                zkClient.delete().forPath(parent + "/" + childId);
            }
        } catch (Exception e) {
            log.error("Failed to delete child: " + parent + "/" + childId, e);
        }
    }

    @Override
    /**
     * Delete a path from zookeeper
     */
    public void delete(String path) {
        try {
            if (zkClient.checkExists().forPath(path) != null) {
                zkClient.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            log.error("Failed to delete child: " + path, e);
        }
    }

    @Override
    /**
     * Get all children beneath a zookeeper parent node
     */
    public List<String> getChildrenNames(String path) {
        try {
            if (zkClient.checkExists().forPath(path) != null) {
                return zkClient.getChildren().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Failed to get children for path: " + path, e);
            return null;
        }
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

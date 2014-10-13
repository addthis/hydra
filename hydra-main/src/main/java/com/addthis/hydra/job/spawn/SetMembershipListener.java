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
package com.addthis.hydra.job.spawn;

import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.hydra.job.SetMembershipAdditionListener;
import com.addthis.hydra.job.SetMembershipRemovalListener;

import com.google.common.collect.ImmutableSet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetMembershipListener implements PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(SetMembershipListener.class);

    private Set<String> memberSet;
    private final String path;

    private final ReentrantLock membershipLock = new ReentrantLock();
    private final CuratorFramework zkClient;
    private final PathChildrenCache cache;
    private final List<SetMembershipAdditionListener> setMembershipAdditionListeners = new CopyOnWriteArrayList<>();
    private final List<SetMembershipRemovalListener> setMembershipRemovedListeners = new CopyOnWriteArrayList<>();


    public SetMembershipListener(CuratorFramework zkClient, String path) {
        this.memberSet = ImmutableSet.of();
        this.path = path;
        this.zkClient = zkClient;
        this.cache = new PathChildrenCache(zkClient, path, true);
        followGroup();
    }

    public Set<String> getMemberSet() {
        membershipLock.lock();
        try {
            return memberSet == null ? ImmutableSet.<String>of() : ImmutableSet.copyOf(memberSet);
        } finally {
            membershipLock.unlock();
        }
    }

    public int getMemberSetSize() {
        membershipLock.lock();
        try {
            return memberSet == null ? 0 : memberSet.size();
        } finally {
            membershipLock.unlock();
        }
    }

    protected void followGroup() {
        if (zkClient != null) {
            membershipLock.lock();
            try {
                this.cache.start();
                this.cache.getListenable().addListener(this);
                if (zkClient.checkExists().forPath(path) == null) {
                    try {
                        zkClient.create().creatingParentsIfNeeded().forPath(path, null);
                    } catch (KeeperException.NodeExistsException e) {
                        // do nothing if it already exists
                    }
                }
                memberSet = getCurrentMembers();
                log.info("Initial members for path: " + path + " are: " + memberSet);
            } catch (Exception ex) {
                log.warn("Failed to pre-load members for path: " + path, ex);
            } finally {
                membershipLock.unlock();
            }
        } else {
            log.warn("no zkclient, not following group: " + path);
        }
    }

    protected void shutdown() throws IOException {
        cache.close();
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
        membershipLock.lock();
        try {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    String memberAdded = ZKPaths.getNodeFromPath(event.getData().getPath());
                    if (setMembershipAdditionListeners.size() > 0) {
                        for (SetMembershipAdditionListener setMembershipAdditionListener : setMembershipAdditionListeners) {
                            setMembershipAdditionListener.memberAdded(memberAdded);
                        }
                    }
                    if (zkClient.getState() == CuratorFrameworkState.STARTED) {
                        memberSet = getCurrentMembers();
                    }
                    break;
                }
                case CHILD_REMOVED: {
                    String memberRemoved = ZKPaths.getNodeFromPath(event.getData().getPath());
                    if (setMembershipRemovedListeners.size() > 0) {
                        for (SetMembershipRemovalListener setMembershipRemovalListener : setMembershipRemovedListeners) {
                            setMembershipRemovalListener.memberRemoved(memberRemoved);
                        }
                    }
                    if (zkClient.getState() == CuratorFrameworkState.STARTED) {
                        memberSet = getCurrentMembers();
                    }
                    break;
                }

                default:
                    log.debug("Unhandled event type:" + event.getType());
            }
        } catch (Exception e) {
            log.error("Error updating member list", e);
        } finally {
            membershipLock.unlock();
        }
    }

    private Set<String> getCurrentMembers() throws Exception {
        List<String> currentMembers = zkClient.getChildren().forPath(path);
        return currentMembers == null ? new HashSet<String>() : ImmutableSet.copyOf(currentMembers);
    }

}

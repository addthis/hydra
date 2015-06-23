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

import javax.annotation.Nonnull;

import java.io.IOException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableSet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetMembershipListener implements PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(SetMembershipListener.class);

    private Set<String> memberSet;

    @Nonnull private final CuratorFramework zkClient;
    @Nonnull private final String path;
    @Nonnull private final ReentrantLock membershipLock;
    @Nonnull private final PathChildrenCache cache;

    public SetMembershipListener(@Nonnull CuratorFramework zkClient, @Nonnull String path) {
        this.path = path;
        this.zkClient = zkClient;
        this.membershipLock = new ReentrantLock();
        this.memberSet = ImmutableSet.of();
        this.cache = new PathChildrenCache(zkClient, path, true);
        followGroup();
    }

    public Set<String> getMemberSet() {
        membershipLock.lock();
        try {
            if (memberSet == null) {
                return ImmutableSet.of();
            } else {
                return ImmutableSet.copyOf(memberSet);
            }
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

    protected final void followGroup() {
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
            log.info("Initial members for path: {} are: {}", path, memberSet);
        } catch (Exception ex) {
            log.warn("Failed to pre-load members for path: {}", path, ex);
        } finally {
            membershipLock.unlock();
        }
    }

    protected void shutdown() throws IOException {
        cache.close();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        membershipLock.lock();
        try {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    if (zkClient.getState() == CuratorFrameworkState.STARTED) {
                        memberSet = getCurrentMembers();
                    }
                    break;
                }
                case CHILD_REMOVED: {
                    if (zkClient.getState() == CuratorFrameworkState.STARTED) {
                        memberSet = getCurrentMembers();
                    }
                    break;
                }
                default:
                    log.debug("Unhandled event type:{}", event.getType());
            }
        } catch (Exception e) {
            log.error("Error updating member list", e);
        } finally {
            membershipLock.unlock();
        }
    }

    private Set<String> getCurrentMembers() throws Exception {
        List<String> currentMembers = zkClient.getChildren().forPath(path);
        if (currentMembers == null) {
            return ImmutableSet.of();
        } else {
            return ImmutableSet.copyOf(currentMembers);
        }
    }

}

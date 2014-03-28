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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.bark.ZkClientFactory;
import com.addthis.bark.ZkGroupMembership;
import com.addthis.hydra.mq.SessionExpireListener;
import com.addthis.hydra.mq.ZkSessionExpirationHandler;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;


import org.slf4j.LoggerFactory;
public class SetMembershipListener implements IZkChildListener, ZkSessionExpirationHandler {

    private static Logger log = LoggerFactory.getLogger(SetMembershipListener.class);

    private Set<String> memberSet;
    private final String path;

    private final ReentrantLock membershipLock = new ReentrantLock();
    private final ZkClient zkClient;
    private boolean ephemeral;
    private final List<SetMembershipAdditionListener> setMembershipAdditionListeners = new CopyOnWriteArrayList<SetMembershipAdditionListener>();
    private final List<SetMembershipRemovalListener> setMembershipRemovedListeners = new CopyOnWriteArrayList<SetMembershipRemovalListener>();


    public SetMembershipListener(String path, boolean ephemeral) {
        this.memberSet = ImmutableSet.of();
        this.path = path;
        this.zkClient = ZkClientFactory.makeStandardClient();
        zkClient.subscribeStateChanges(new SessionExpireListener(this));
        this.ephemeral = ephemeral;
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

    public void addAdditionListener(SetMembershipAdditionListener setMembershipAdditionListener) {
        setMembershipAdditionListeners.add(setMembershipAdditionListener);
    }

    public boolean removeAdditionListener(SetMembershipAdditionListener setMembershipAdditionListener) {
        return setMembershipAdditionListeners.remove(setMembershipAdditionListener);
    }

    public void addRemovalListener(SetMembershipRemovalListener setMembershipRemovalListener) {
        setMembershipRemovedListeners.add(setMembershipRemovalListener);
    }

    public boolean removeRemovalListener(SetMembershipRemovalListener setMembershipRemovalListener) {
        return setMembershipRemovedListeners.remove(setMembershipRemovalListener);
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        membershipLock.lock();
        try {
            Set<String> members = currentChilds == null ? new HashSet<String>() : ImmutableSet.copyOf(currentChilds);
            Set<String> newMembers = Sets.difference(members, memberSet);
            Set<String> removedMembers = Sets.difference(memberSet, members);
            if (newMembers.size() > 0 || removedMembers.size() > 0) {
                if (newMembers.size() > 0 && setMembershipAdditionListeners.size() > 0) {
                    for (SetMembershipAdditionListener setMembershipAdditionListener : setMembershipAdditionListeners) {
                        for (String newMember : newMembers) {
                            setMembershipAdditionListener.memberAdded(newMember);
                        }
                    }
                }
                if (removedMembers.size() > 0 && setMembershipRemovedListeners.size() > 0) {
                    for (SetMembershipRemovalListener setMembershipRemovalListener : setMembershipRemovedListeners) {
                        for (String removedMember : removedMembers) {
                            setMembershipRemovalListener.memberRemoved(removedMember);
                        }
                    }
                }
                log.warn("[membership change] " + path + " added: " + newMembers.toString() + " removed: " + removedMembers.toString());
            }
            memberSet = members;
        } finally {
            membershipLock.unlock();
        }
    }

    @Override
    public void handleExpiredSession() {
        followGroup();
    }

    protected void followGroup() {
        if (zkClient != null) {
            membershipLock.lock();
            try {
                ZkGroupMembership zkGroupMembership = new ZkGroupMembership(zkClient, ephemeral);
                List<String> currentMembers = zkGroupMembership.listenToGroup(path, this);
                log.info("Initial members for path: " + path + " are: " + currentMembers);
                if (currentMembers != null) {
                    this.memberSet = ImmutableSet.copyOf(currentMembers);
                }
            } catch (Exception ex) {
                log.warn("Failed to pre-load members for path: " + path, ex);
            } finally {
                membershipLock.unlock();
            }
        } else {
            log.warn("no zkclient, not following group: " + path);
        }
    }

}

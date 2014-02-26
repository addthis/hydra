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

import com.addthis.bark.ZkStartUtil;
import com.addthis.basis.test.SlowTest;
import com.google.common.collect.ImmutableSet;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
public class SetMembershipListenerTest extends ZkStartUtil {

    @Test
    public void testAvailableMinions() throws Exception {
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/minion/up/foo");
        SetMembershipListener setMembershipListener = new SetMembershipListener(zkClient, "/minion/up");
        assertEquals(ImmutableSet.of("foo"), setMembershipListener.getMemberSet());
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/minion/up/bar");
        Thread.sleep(1000);
        assertEquals(ImmutableSet.of("foo", "bar"), setMembershipListener.getMemberSet());
        zkClient.delete().forPath("/minion/up/foo");
        Thread.sleep(1000);
        assertEquals(ImmutableSet.of("bar"), setMembershipListener.getMemberSet());
        setMembershipListener.shutdown();
    }
}

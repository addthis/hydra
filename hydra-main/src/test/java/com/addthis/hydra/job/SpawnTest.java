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

import java.util.Arrays;
import java.util.Map;

import com.addthis.basis.test.SlowTest;

import com.addthis.bark.ZkStartUtil;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;

import org.apache.zookeeper.CreateMode;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class SpawnTest extends ZkStartUtil {

    // todo: use random temp dirs
    @Before
    public void setParams() {
        System.setProperty("SPAWN_DATA_DIR", "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", "/tmp/spawn/log/events");
    }

    @Test
    public void jobTaskPersistenceTest() throws Exception {
        int taskId = 3;
        String jobId = "somejob";
        JobTask testTask = new JobTask();
        testTask.setTaskID(taskId);
        testTask.setJobUUID(jobId);
        CodecJSON codec = new CodecJSON();
        byte[] encoded = codec.encode(testTask);
        JobTask decodedTask = new JobTask();
        codec.decode(decodedTask, encoded);
        assertEquals(decodedTask.getJobUUID(), jobId);
        assertEquals(decodedTask.getTaskID(), taskId);
        assertEquals(decodedTask.getJobKey(), new JobKey(jobId, taskId));

    }

    @Test
    public void toggleHostTest() throws Exception {
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/minion/up");
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/minion/dead");
        HostState tmpDisableHostState = createHostState("tmp");
        tmpDisableHostState.setDisabled(true);
        assertTrue("disabled host states should not be able to run tasks", !tmpDisableHostState.canMirrorTasks());
        Spawn spawn = new Spawn();
        HostState toggleHost = createHostState("toggle");
        HostState otherHost = createHostState("other");
        spawn.updateHostState(toggleHost);
        spawn.updateHostState(otherHost);
        spawn.toggleHosts(toggleHost.getHost(), true);
        assertTrue("toggled host should be disabled", spawn.getHostState(toggleHost.getHostUuid()).isDisabled());
        assertTrue("other host should not be disabled", !spawn.getHostState(otherHost.getHostUuid()).isDisabled());
        spawn.toggleHosts(toggleHost.getHost(), false);
        assertTrue("toggled host should now be re-enabled", !spawn.getHostState(toggleHost.getHostUuid()).isDisabled());
    }

    @Test
    public void testMacroFindParameters() throws Exception {
        Spawn spawn = new Spawn();
        Map<String, JobParameter> params = JobExpand.macroFindParameters("%[foobar:5]%");
        assertEquals(1, params.size());
        assertTrue(params.containsKey("foobar"));

        params = JobExpand.macroFindParameters("\"%[foobar:5]%\"");
        assertEquals(1, params.size());
        assertTrue(params.containsKey("foobar"));

        params = JobExpand.macroFindParameters("// \"%[foobar:5]%\" ");
        assertEquals(0, params.size());

        params = JobExpand.macroFindParameters("/* \"%[foobar:5]%\" */");
        assertEquals(0, params.size());

        // Missing trailing ']%' characters
        params = JobExpand.macroFindParameters("%[foobar:5");
        assertEquals(0, params.size());
    }

    @Test
    public void testMacroExpandParameters() throws Exception {
        Spawn spawn = new Spawn();
        Map<String, JobParameter> params = JobExpand.macroFindParameters("%[a:foo]% %[b:bar]%");
        assertEquals(2, params.size());
        assertTrue(params.containsKey("a"));
        assertTrue(params.containsKey("b"));

        String jobConfig = "%[a:foo]% %[b:bar]%";

        String output = JobExpand.macroTemplateParams(jobConfig, params.values());
        assertEquals("foo bar", output);

        params.get("a").setValue("hello");
        params.get("b").setValue("world");

        output = JobExpand.macroTemplateParams(jobConfig, params.values());
        assertEquals("hello world", output);

        jobConfig = "%[a:foo]% // %[b:bar]%";
        output = JobExpand.macroTemplateParams(jobConfig, params.values());
        assertEquals("hello // %[b:bar]%", output);

        jobConfig = "%[a:foo]% %[b]%";
        params = JobExpand.macroFindParameters(jobConfig);
        output = JobExpand.macroTemplateParams(jobConfig, params.values());
        assertEquals("foo ", output);
    }

    @Test
    public void fixDirsTest() throws Exception {
        Spawn spawn = new Spawn(zkClient);
        spawn.setSpawnMQ(EasyMock.createNiceMock(SpawnMQImpl.class));
        HostState host0 = createHostState("host0");
        spawn.updateHostState(host0);
        HostState host1 = createHostState("host1");
        spawn.updateHostState(host1);
        spawn.putCommand("c", new JobCommand(), false);
        Job job = spawn.createJob("fsm", 3, Arrays.asList("host0"), "default", "c");
        job.setReplicas(1);
        spawn.rebalanceReplicas(job);
        host0.setStopped(new JobKey[]{new JobKey(job.getId(), 0)});
        spawn.updateHostState(host0);
        host1.setStopped(new JobKey[]{new JobKey(job.getId(), 0), new JobKey(job.getId(), 1)});
        spawn.updateHostState(host1);
        HostState host2 = createHostState("host2");
        host2.setStopped(new JobKey[] {new JobKey(job.getId(), 2)});
        spawn.updateHostState(host2);
        boolean hostsAreUp = false;
        for (int i=0; i<10; i++) {
            if (spawn.listHostStatus(null).size() < 3) {
                Thread.sleep(1000); // Can take a little while for the hosts to appear as up
            } else {
                hostsAreUp = true;
                break;
            }
        }
        if (!hostsAreUp) {
            throw new RuntimeException("Failed to find hosts after waiting");
        }
        assertEquals("should not change task that is on on both hosts", 0, spawn.fixTaskDir(job.getId(), 0, false, false).get("tasksChanged"));
        assertEquals("should copy task that is on only one host", 1, spawn.fixTaskDir(job.getId(), 1, false, false).get("tasksChanged"));
        assertEquals("new home for task 1 should be the host that had the directory", "host1", spawn.getTask(job.getId(), 1).getHostUUID());
        assertEquals("should copy task that is on an unexpected host", 1, spawn.fixTaskDir(job.getId(), 2, false, false).get("tasksChanged"));
        assertEquals("new home for task 2 should be the unexpected host that had the directory", "host2", spawn.getTask(job.getId(), 2).getHostUUID());
    }

    private HostState createHostState(String hostUUID) throws Exception {
        String zkPath = "/minion/up/" + hostUUID;
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkPath);
        HostState origState = new HostState(hostUUID);
        origState.setMax(new HostCapacity(10, 10, 10));
        origState.setHost("hostname-for:" + hostUUID);
        origState.setReplicas(new JobKey[1]);
        origState.setStopped(new JobKey[1]);
        origState.setUp(true);
        return origState;
    }
}

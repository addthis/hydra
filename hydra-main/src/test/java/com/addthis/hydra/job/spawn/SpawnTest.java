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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.test.SlowTest;

import com.addthis.codec.config.Configs;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.alert.JobAlertManager;
import com.addthis.hydra.job.auth.PermissionsManager;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.entity.JobCommandManager;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.StatusTaskEnd;
import com.addthis.hydra.job.web.JobRequestHandlerImpl;
import com.addthis.hydra.util.ZkCodecStartUtil;

import org.apache.zookeeper.CreateMode;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SlowTest.class)
public class SpawnTest extends ZkCodecStartUtil {

    private Spawn spawn;
    private JobCommandManager jobCommandManager;
    private JobAlertManager jobAlertManager;
    private JobRequestHandlerImpl impl;
    private String username = "megatron";
    private String token = "megatron";
    private String sudo = null;
    private KVPairs kv;

    @Before
    public void setUp() {
        System.setProperty("SPAWN_DATA_DIR", "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", "/tmp/spawn/log/events");

        // mocks and stubs
        spawn = mock(Spawn.class);
        jobCommandManager = mock(JobCommandManager.class);
        jobAlertManager = mock(JobAlertManager.class);
        when(spawn.getJobCommandManager()).thenReturn(jobCommandManager);
        when(spawn.getJobAlertManager()).thenReturn(jobAlertManager);
        when(spawn.getPermissionsManager()).thenReturn(PermissionsManager.createManagerAllowAll());
        when(jobCommandManager.getEntity("default-task")).thenReturn(new JobCommand());

        impl = new JobRequestHandlerImpl(spawn);
        kv = new KVPairs();
    }

    @Test
    public void failJobCountTest() throws Exception {
        Job job = new Job("new_job_id", "megatron");
        when(spawn.createJob("megatron", -1, Collections.<String> emptyList(), "default", "default-task", false)).thenReturn(job);
        when(spawn.getJob("new_job_id")).thenReturn(job);

        JobTask task = new JobTask("a", 0, 0);

        StatusTaskEnd update = mock(StatusTaskEnd.class);
        when(update.getExitCode()).thenReturn(1);

        spawn.handleTaskError(job, task, update.getExitCode());
        spawn.handleStatusTaskEnd(job, task, update);



        System.out.println(task.getErrors());


    }

    @Test
    public void jobTaskPersistenceTest() throws Exception {
        int taskId = 3;
        String jobId = "somejob";
        JobTask testTask = new JobTask();
        testTask.setTaskID(taskId);
        testTask.setJobUUID(jobId);
        CodecJSON codec = CodecJSON.INSTANCE;
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
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {
            HostState toggleHost = createHostState("toggle");
            HostState otherHost = createHostState("other");
            spawn.hostManager.updateHostState(toggleHost);
            spawn.hostManager.updateHostState(otherHost);
            spawn.toggleHosts(toggleHost.getHost(), true);
            assertTrue("toggled host should be disabled", spawn.hostManager.getHostState(toggleHost.getHostUuid())
                                                                           .isDisabled());
            assertTrue("other host should not be disabled", !spawn.hostManager.getHostState(otherHost.getHostUuid())
                                                                              .isDisabled());
            spawn.toggleHosts(toggleHost.getHost(), false);
            assertTrue("toggled host should now be re-enabled",
                       !spawn.hostManager.getHostState(toggleHost.getHostUuid()).isDisabled());
        }
    }

    @Test
    public void testMacroFindParameters() throws Exception {
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
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {
            spawn.setSpawnMQ(mock(SpawnMQImpl.class));
            HostState host0 = createHostState("host0");
            spawn.hostManager.updateHostState(host0);
            HostState host1 = createHostState("host1");
            spawn.hostManager.updateHostState(host1);
            spawn.getJobCommandManager().putEntity("c", new JobCommand(), false);
            Job job = spawn.createJob("fsm", 3, Arrays.asList("host0"), "default", "c", false);
            job.setReplicas(1);
            spawn.rebalanceReplicas(job);
            for (JobTask task : job.getCopyOfTasks()) {
                // Convince spawn these tasks have data
                task.setByteCount(1000l);
                task.setFileCount(10);
            }
            spawn.updateJob(job);
            // at this point all tasks are live on host0, with replica on host1
            // now setup hosts for misplaced task replicas:
            // task 0 on host0 and host1 (as should be)
            // task 1 on host1 (missing on host0)
            // task 2 on host2 (missing on host0 and host1)
            host0.setStopped(new JobKey[]{new JobKey(job.getId(), 0)});
            spawn.hostManager.updateHostState(host0);
            host1.setStopped(new JobKey[]{new JobKey(job.getId(), 0), new JobKey(job.getId(), 1)});
            spawn.hostManager.updateHostState(host1);
            HostState host2 = createHostState("host2");
            host2.setStopped(new JobKey[]{new JobKey(job.getId(), 2)});
            spawn.hostManager.updateHostState(host2);
            // Wait for all hosts to be up due to time needed to pick up zk minion/up change. That matters because
            // HostMnager.listHostStatus may set HostState.up to false depending on zk minion/up data, which may
            // affect test results below
            boolean hostsAreUp = false;
            for (int i = 0; i < 10; i++) {
                if (spawn.hostManager.listHostStatus(null).stream().allMatch(host -> host.isUp())) {
                    hostsAreUp = true;
                    break;
                } else {
                    Thread.sleep(1000);
                }
            }
            if (!hostsAreUp) {
                throw new RuntimeException("Failed to find hosts after waiting");
            }
            assertEquals("should not change task that is on on both hosts", 0,
                         spawn.fixTaskDir(job.getId(), 0, false, false).get("tasksChanged"));
            assertEquals("should copy task that is on only one host", 1,
                         spawn.fixTaskDir(job.getId(), 1, false, false).get("tasksChanged"));
            assertEquals("new home for task 1 should be the host that had the directory", "host1",
                         spawn.getTask(job.getId(), 1).getHostUUID());
            assertEquals("should copy task that is on an unexpected host", 1,
                         spawn.fixTaskDir(job.getId(), 2, false, false).get("tasksChanged"));
            assertEquals("new home for task 2 should be the unexpected host that had the directory", "host2",
                         spawn.getTask(job.getId(), 2).getHostUUID());
        }
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
        origState.setUsed(new HostCapacity(0, 0, 0, 0));
        origState.setMax(new HostCapacity(0, 0, 0, 700_000_000_000L + 1));
        origState.setMinionTypes("default,g8");
        return origState;
    }
}

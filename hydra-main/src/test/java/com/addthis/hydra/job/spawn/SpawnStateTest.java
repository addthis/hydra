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

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import com.addthis.basis.util.Files;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import org.junit.After;
import org.junit.Test;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_COMMAND_PATH;
import static org.junit.Assert.assertEquals;


// Tests that involve lots of state and setup.
public class SpawnStateTest extends ZkStartUtil {

    File logDir;

    @Override
    protected void onAfterZKStart() {
        try {
            logDir = Files.createTempDir();
            System.setProperty("SPAWN_LOG_DIR", logDir.getCanonicalPath());
            SpawnDataStore spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
            spawnDataStore.delete(SPAWN_COMMON_COMMAND_PATH); // Clear out command path
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteDir(logDir);
    }

    @Test
    public void testJobConfigs() throws Exception {
        Spawn spawn = new Spawn(zkClient);

        JobConfigManager jobConfigManager = new JobConfigManager(spawn.getSpawnDataStore());
        String conf1 = "{myjob:[1,2,3]}";
        jobConfigManager.setConfig("id", conf1);
        assertEquals("JobConfigManager should correctly put/fetch configs", conf1, jobConfigManager.getConfig("id"));

        String config = "// MY JOB CONFIG";
        spawn.getJobCommandManager().putEntity("a", new JobCommand(), true);
        HostState host = new HostState("h");
        host.setUp(true);
        host.setDead(false);
        spawn.updateHostState(host);
        Job job = spawn.createJob("fsm", 1, Arrays.asList("h"), null, "a");
        job.setReplicas(0);
        spawn.setJobConfig(job.getId(), config);
        spawn.updateJob(job);
        String jobId = job.getId();
        assertEquals("updateJob pipeline should correctly put/fetch configs", config, spawn.getJobConfig(jobId));
    }

    @Test
    public void testEmptyCommands() throws Exception {
        File tmpRoot = Files.createTempDir();
        System.setProperty("SPAWN_DATA_DIR", tmpRoot + "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", tmpRoot + "/tmp/spawn/log/events");
        try (Spawn spawn = new Spawn(zkClient)) {
            assertEquals(0, spawn.getJobCommandManager().size());
        } finally {
            Files.deleteDir(tmpRoot);
        }
    }

    private JobCommand jcmd;

    @Test
    public void testGetCommand() throws Exception {
        File tmpRoot = Files.createTempDir();
        System.setProperty("SPAWN_DATA_DIR", tmpRoot + "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", tmpRoot + "/tmp/spawn/log/events");
        try (Spawn spawn = new Spawn(zkClient)) {
            jcmd = new JobCommand("me", new String[]{"ls"}, 1, 1, 1);
            spawn.getJobCommandManager().putEntity("test1", jcmd, true);
            assertEquals(1, spawn.getJobCommandManager().size());
            // todo: brittle, but this is the easiest way to test job equality right now
            assertEquals(jcmd.toJSON().toString(),
                         spawn.getJobCommandManager().getEntity("test1").toJSON().toString());
        } finally {
            Files.deleteDir(tmpRoot);
        }
    }

    @Test
    public void testBalanceConfigSet() throws Exception {
        File tmpRoot = Files.createTempDir();
        System.setProperty("SPAWN_DATA_DIR", tmpRoot + "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", tmpRoot + "/tmp/spawn/log/events");
        try (Spawn spawn = new Spawn(zkClient)) {
            SpawnBalancerConfig config = new SpawnBalancerConfig();
            long newVal = 123456;
            config.setBytesMovedFullRebalance(newVal);
            spawn.updateSpawnBalancerConfig(config);
            spawn.writeSpawnBalancerConfig();
            assertEquals("expected to see updated balancer value", newVal,
                         spawn.getSpawnBalancer().getConfig().getBytesMovedFullRebalance());
            spawn.loadSpawnBalancerConfig();
            assertEquals("expect to see changed balance parameter persisted", 123456l,
                         spawn.getSpawnBalancer().getConfig().getBytesMovedFullRebalance());
        } finally {
            Files.deleteDir(tmpRoot);
        }
    }
}

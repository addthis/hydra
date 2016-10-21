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

import com.addthis.basis.util.LessFiles;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.JobExpander;
import com.addthis.hydra.job.JobExpanderImpl;
import com.addthis.hydra.job.alert.SuppressChanges;
import com.addthis.hydra.job.alert.types.OnErrorJobAlert;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.util.ZkCodecStartUtil;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Test;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_COMMAND_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


// Tests that involve lots of state and setup.
public class SpawnStateTest extends ZkCodecStartUtil {

    File logDir;

    @Override
    protected void onAfterZKStart() {
        try {
            logDir = LessFiles.createTempDir();
            System.setProperty("SPAWN_LOG_DIR", logDir.getCanonicalPath());
            SpawnDataStore spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
            spawnDataStore.delete(SPAWN_COMMON_COMMAND_PATH); // Clear out command path
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanup() throws IOException {
        LessFiles.deleteDir(logDir);
    }

    @Test
    public void testJobConfigs() throws Exception {
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {
            JobExpander jobExpander = new JobExpanderImpl(spawn, spawn.getJobMacroManager(), spawn.getAliasManager());
            JobConfigManager jobConfigManager = new JobConfigManager(spawn.getSpawnDataStore(), jobExpander);
            String conf1 = "{myjob:[1,2,3]}";
            jobConfigManager.setConfig("id", conf1);
            assertEquals("JobConfigManager should correctly put/fetch configs", conf1,
                         jobConfigManager.getConfig("id"));

            String config = "// MY JOB CONFIG";
            spawn.getJobCommandManager().putEntity("a", new JobCommand(), true);
            HostState host = new HostState("h");
            host.setUp(true);
            host.setDead(false);
            spawn.hostManager.updateHostState(host);
            Job job = spawn.createJob("fsm", 1, Arrays.asList("h"), null, "a", false);
            job.setReplicas(0);
            spawn.setJobConfig(job.getId(), config);
            spawn.updateJob(job);
            String jobId = job.getId();
            assertEquals("updateJob pipeline should correctly put/fetch configs", config, spawn.getJobConfig(jobId));
        }
    }

    @Test
    public void testAutomaticAlertDeletion() throws Exception {
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {

            spawn.getJobCommandManager().putEntity("a", new JobCommand(), true);
            HostState host = new HostState("h");
            host.setUp(true);
            host.setDead(false);
            spawn.hostManager.updateHostState(host);
            Job job = spawn.createJob("fsm", 1, Arrays.asList("h"), null, "a", false);
            OnErrorJobAlert input = new OnErrorJobAlert(null, "foobar", 0, "foobar@localhost", null,
                                                        ImmutableList.of(job.getId()),
                                                        SuppressChanges.FALSE, 0, null, null);
            spawn.getJobAlertManager().putAlert(input.alertId, input);
            String json = spawn.getJobAlertManager().getAlert(input.alertId);
            OnErrorJobAlert output = Configs.decodeObject(OnErrorJobAlert.class, json);
            assertEquals(input.alertId, output.alertId);
            spawn.deleteJob(job.getId());
            assertNull(spawn.getJobAlertManager().getAlert(input.alertId));
        }
    }

    @Test
    public void testAutomaticAlertReduction() throws Exception {
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {

            spawn.getJobCommandManager().putEntity("a", new JobCommand(), true);
            HostState host = new HostState("h");
            host.setUp(true);
            host.setDead(false);
            spawn.hostManager.updateHostState(host);
            Job job = spawn.createJob("fsm", 1, Arrays.asList("h"), null, "a", false);
            OnErrorJobAlert input = new OnErrorJobAlert(null, "foobar", 0, "foobar@localhost", null,
                                                        ImmutableList.of(job.getId(), "foobar"),
                                                        SuppressChanges.FALSE, 0, null, null);
            spawn.getJobAlertManager().putAlert(input.alertId, input);
            String json = spawn.getJobAlertManager().getAlert(input.alertId);
            OnErrorJobAlert output = Configs.decodeObject(OnErrorJobAlert.class, json);
            assertEquals(input.alertId, output.alertId);
            assertEquals(2, output.jobIds.size());
            spawn.deleteJob(job.getId());
            json = spawn.getJobAlertManager().getAlert(input.alertId);
            output = Configs.decodeObject(OnErrorJobAlert.class, json);
            assertEquals(input.alertId, output.alertId);
            assertEquals(1, output.jobIds.size());
            assertTrue(output.jobIds.contains("foobar"));
        }
    }

    @Test
    public void testEmptyCommands() throws Exception {
        File tmpRoot = LessFiles.createTempDir();
        System.setProperty("SPAWN_DATA_DIR", tmpRoot + "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", tmpRoot + "/tmp/spawn/log/events");
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {
            assertEquals(0, spawn.getJobCommandManager().size());
        } finally {
            LessFiles.deleteDir(tmpRoot);
        }
    }

    private JobCommand jcmd;

    @Test
    public void testGetCommand() throws Exception {
        File tmpRoot = LessFiles.createTempDir();
        System.setProperty("SPAWN_DATA_DIR", tmpRoot + "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", tmpRoot + "/tmp/spawn/log/events");
        try (Spawn spawn = Configs.newDefault(Spawn.class)) {
            jcmd = new JobCommand("me", new String[]{"ls"}, 1, 1, 1);
            spawn.getJobCommandManager().putEntity("test1", jcmd, true);
            assertEquals(1, spawn.getJobCommandManager().size());
            // todo: brittle, but this is the easiest way to test job equality right now
            assertEquals(jcmd.toJSON().toString(),
                         spawn.getJobCommandManager().getEntity("test1").toJSON().toString());
        } finally {
            LessFiles.deleteDir(tmpRoot);
        }
    }

}

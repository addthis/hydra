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

import com.addthis.basis.test.SlowTest;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.job.mq.HostCapacity;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class HostFailWorkerTest extends ZkStartUtil {

    private Spawn spawn;
    private HostFailWorker hostFailWorker;

    @Before
    public void setup() throws Exception {
        spawn = new Spawn(zkClient);
        hostFailWorker = spawn.getHostFailWorker();
    }

    @After
    public void cleanup() throws Exception {
        hostFailWorker.stop();
        spawn.getSpawnDataStore().close();
    }

    @Test
    public void minionDownTest() throws Exception {
        String liveHostId = "livehost";
        String replicaHostId = "replicahost";
        String emptyHostId = "emptyhost";
        spawn.updateHostState(makeHostState("otherhost", true));
        // If a minion is down, HostFailWorker should complain
        spawn.updateHostState(makeHostState(emptyHostId, true));
        HostState host1 = makeHostState(liveHostId, true);
        Job job = makeSingleTaskJob(liveHostId, replicaHostId);
        JobKey[] jobKeys = new JobKey[]{new JobKey(job.getId(), 0)};
        host1.setStopped(jobKeys);
        spawn.updateHostState(host1);
        HostState host2 = makeHostState("downhost", false);
        host2.setReplicas(jobKeys);
        spawn.updateHostState(host2);
        assertTrue("should not be able to fail host with task that is also on a down host", !hostFailWorker.checkHostStatesForFailure(liveHostId));
        assertTrue("should be able to fail empty host", hostFailWorker.checkHostStatesForFailure(emptyHostId));
    }

    private Job makeSingleTaskJob(String liveHost, String replicaHost) throws Exception {
        Job job = spawn.createJob("a", 0, Arrays.asList(liveHost, replicaHost), "default", "dummy");
        JobTask task = new JobTask(liveHost, 0, 0);
        task.setReplicas(Arrays.asList(new JobTaskReplica(replicaHost, job.getId(), 0, 0)));
        job.setTasks(Arrays.asList(task));
        spawn.updateJob(job, false);
        return job;
    }

    @Test
    public void statePersistenceTest() {
        // Mark some hosts for failure, then spin up a new HostFailWorker and make sure it can load the state
        hostFailWorker.markHostsToFail("a,b", HostFailWorker.FailState.FAILING_FS_DEAD);
        hostFailWorker.markHostsToFail("c", HostFailWorker.FailState.FAILING_FS_OKAY);
        HostFailWorker hostFailWorker2 = new HostFailWorker(spawn);
        assertEquals("should persist state", HostFailWorker.FailState.FAILING_FS_DEAD, hostFailWorker2.getFailureState("a"));
        assertEquals("should persist state", HostFailWorker.FailState.FAILING_FS_DEAD, hostFailWorker2.getFailureState("b"));
        assertEquals("should persist state", HostFailWorker.FailState.FAILING_FS_OKAY, hostFailWorker2.getFailureState("c"));
        assertEquals("should show alive state for non-failed host", HostFailWorker.FailState.ALIVE, hostFailWorker2.getFailureState("d"));
        hostFailWorker2.stop();
    }

    @Test
    public void failWarningTest() throws JSONException {
        // Check to make sure the UI warnings about host state are correct
        spawn.updateHostState(makeHostState("a", true, 1000, 2000));
        spawn.updateHostState(makeHostState("b", true, 500, 2000));
        spawn.updateHostState(makeHostState("c", true, 1500, 2000));
        JSONObject failBMessage = hostFailWorker.getInfoForHostFailure("b", true);
        assertTrue("should have ~.5 disk usage before fail", failBMessage.get("prefail") != null && areClose(failBMessage.getDouble("prefail"), .5));
        assertTrue("should have ~.75 disk usage after fail", failBMessage.get("postfail") != null && areClose(failBMessage.getDouble("postfail"), .75));
        spawn.updateHostState(makeHostState("d", true, 10000, 11000)); // This host has more disk used than would fit on the other hosts
        JSONObject failDMessage = hostFailWorker.getInfoForHostFailure("d", true);
        assertTrue("should get fatal warning after failing too-big host", failDMessage.has("fatal"));
    }

    @Test
    public void fullDiskTest() throws Exception {
        String fullHostId = "full";
        String emptyHostId = "empty";
        spawn.updateHostState(makeHostState(fullHostId, true, 999, 1000));
        spawn.updateHostState(makeHostState(emptyHostId, true, 100, 1000));
        zkClient.create().creatingParentsIfNeeded().forPath("/minion/up/" + fullHostId, new byte[]{});
        zkClient.create().creatingParentsIfNeeded().forPath("/minion/up/" + emptyHostId, new byte[]{});
        Thread.sleep(2000); // need to let spawn detect the new minions
        hostFailWorker.updateFullMinions();
        assertEquals("should correctly detect full host", HostFailWorker.FailState.DISK_FULL, hostFailWorker.getFailureState(fullHostId));
        assertEquals("should correctly detect okay host", HostFailWorker.FailState.ALIVE, hostFailWorker.getFailureState(emptyHostId));
    }

    private static boolean areClose(double x, double y) {
        return Math.abs(x - y) < .001;
    }

    private HostState makeHostState(String id, boolean up, long diskUsed, long diskMax) {
        HostState hostState = makeHostState(id, up);
        hostState.setUsed(new HostCapacity(0, 0, 0, diskUsed));
        hostState.setMax(new HostCapacity(0, 0, 0, diskMax));
        return hostState;
    }

    private HostState makeHostState(String id, boolean up) {
        HostState hostState = new HostState(id);
        hostState.setHost("pretend_host");
        hostState.setUp(up);
        hostState.setReplicas(new JobKey[]{});
        hostState.setStopped(new JobKey[]{});
        return hostState;
    }
}

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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpawnQueuesByPriorityTest {

    @Test
    public void hostSortingTest() {
        SpawnQueuesByPriority spawnQueuesByPriority = new SpawnQueuesByPriority();
        spawnQueuesByPriority.put(0, new LinkedList<>());
        HostState noSlots = makeHostState("h1", 0, 0);
        HostState oneSlotHighMeanActive = makeHostState("h2", 1, .95);
        HostState oneSlotLowMeanActive = makeHostState("h3", 1, .03);
        HostState twoSlots = makeHostState("h4", 2, .99);
        List<HostState> allHosts = Arrays.asList(noSlots, oneSlotHighMeanActive, oneSlotLowMeanActive, twoSlots);
        spawnQueuesByPriority.updateAllHostAvailSlots(allHosts);
        // If host with two slots is available, should choose that host.
        assertEquals("should choose host with two slots", twoSlots, spawnQueuesByPriority.findBestHostToRunTask(allHosts, true));
        // If multiple hosts with one slot are available, should choose one with lesser meanActiveTasks.
        assertEquals("should choose less active host", oneSlotLowMeanActive, spawnQueuesByPriority.findBestHostToRunTask(allHosts.subList(0, 3), true));
        // If only available host has no slots, should return null
        assertEquals("should return null", null, spawnQueuesByPriority.findBestHostToRunTask(Arrays.asList(noSlots), true));
        // Simulate a task kicking on the twoSlots host. Then we should choose the oneSlot host with the lowest meanActive value.
        spawnQueuesByPriority.markHostTaskActive(twoSlots.getHostUuid());
        assertEquals("after kick, should return less active host", oneSlotLowMeanActive, spawnQueuesByPriority.findBestHostToRunTask(allHosts, true));
    }

    @Test
    public void queueTest() {
        SpawnQueuesByPriority spawnQueuesByPriority = new SpawnQueuesByPriority();
        JobKey key1 = new JobKey("job", 0);
        spawnQueuesByPriority.addTaskToQueue(0, key1, 0, false);
        JobKey key2 = new JobKey("job", 1);
        spawnQueuesByPriority.addTaskToQueue(0, key2, 0, false);
        JobKey headKey = new JobKey("job", 2);
        spawnQueuesByPriority.addTaskToQueue(0, headKey, 0, true);
        JobKey highPriKey = new JobKey("job2", 10);
        spawnQueuesByPriority.addTaskToQueue(1, highPriKey, 0, false);
        Iterator<JobKey> expected = Arrays.asList(highPriKey, headKey, key1, key2).iterator();
        assertEquals("should get expected number of pri=1 tasks", 1, spawnQueuesByPriority.getTaskQueuedCount(1));
        assertEquals("should get expected number of pri=0 tasks", 3, spawnQueuesByPriority.getTaskQueuedCount(0));
        for (LinkedList<SpawnQueueItem> keyList : spawnQueuesByPriority.values()) {
            for (SpawnQueueItem item : keyList) {
                assertEquals("should get keys in expected order", item.getJobKey(), expected.next().getJobKey());
            }
        }
        long maxTaskBytesToMigrate = SpawnQueuesByPriority.getTaskMigrationMaxBytes();
        long limitGrowthInterval = SpawnQueuesByPriority.getTaskMigrationLimitGrowthInterval();
        // Check that small tasks can migrate soon after being put on the queue, but large tasks have to wait.
        assertTrue("should allow small task to migrate immediately", spawnQueuesByPriority.checkSizeAgeForMigration(0, 0));
        assertTrue("should not allow large task to migrate, even after waiting", !spawnQueuesByPriority.checkSizeAgeForMigration(2 * maxTaskBytesToMigrate, 2 * limitGrowthInterval));
        assertTrue("should not allow medium task to migrate immediately", !spawnQueuesByPriority.checkSizeAgeForMigration(maxTaskBytesToMigrate / 2, 0));
        assertTrue("should allow medium task after waiting", spawnQueuesByPriority.checkSizeAgeForMigration(maxTaskBytesToMigrate / 2, limitGrowthInterval));
        for (String hostName : Arrays.asList("a", "b", "c", "d")) {
            spawnQueuesByPriority.incrementHostAvailableSlots(hostName);
        }
        // Simulate a migration from host a to host b. Make sure that neither a nor b can perform a migration again for a time interval
        spawnQueuesByPriority.markMigrationBetweenHosts("a", "b");
        JobTask task = new JobTask("a", 0, 0);
        task.setByteCount(1);
        assertTrue("should not allow migration from same host immediately", !spawnQueuesByPriority.shouldMigrateTaskToHost(task, "c"));
        JobTask task2 = new JobTask("c", 0, 0);
        task2.setByteCount(1);
        assertTrue("should not allow migration to same host immediately", !spawnQueuesByPriority.shouldMigrateTaskToHost(task2, "b"));
        JobTask task3 = new JobTask("d", 0, 0);
        task3.setByteCount(1);
        assertTrue("should allow migration between distinct hosts", spawnQueuesByPriority.shouldMigrateTaskToHost(task3, "c"));
    }

    private HostState makeHostState(String uuid, int availSlots, double meanActiveTasks) {
        HostState hostState = new HostState(uuid);
        hostState.setAvailableTaskSlots(availSlots);
        hostState.setMeanActiveTasks(meanActiveTasks);
        return hostState;
    }
}

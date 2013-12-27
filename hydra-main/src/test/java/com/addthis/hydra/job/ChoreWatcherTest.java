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

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.test.SlowTest;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.job.chores.Chore;
import com.addthis.hydra.job.chores.ChoreAction;
import com.addthis.hydra.job.chores.ChoreCondition;
import com.addthis.hydra.job.chores.ChoreWatcher;
import com.addthis.hydra.job.chores.IdleChoreCondition;
import com.addthis.hydra.job.chores.PromoteAction;
import com.addthis.hydra.job.chores.SpawnChoreWatcher;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class ChoreWatcherTest extends ZkStartUtil {

    private final String dummyId = "dummyId";
    private ExecutorService choreExecutor;
    SpawnDataStore spawnDataStore;

    @Override
    protected void onAfterZKStart() {
        choreExecutor = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(2, 2, 0l, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("choreExecutor-%d").build()));
    }

    @Override
    protected void onAfterZKStop() {
        choreExecutor.shutdownNow();
    }

    @Before
    public void setParams() throws Exception {
        spawnDataStore = DataStoreUtil.makeSpawnDataStore(myZkClient);
        System.setProperty("SPAWN_DATA_DIR", "/tmp/spawn/data");
        System.setProperty("SPAWN_LOG_DIR", "/tmp/spawn/log/events");
    }

    @Test
    public void testChoreAddFinish() throws Exception {
        ChoreWatcher choreWatcher = new ChoreWatcher(dummyId, spawnDataStore, choreExecutor, 1000);
        Chore chore = new Chore("abc", null, new ArrayList<ChoreAction>(), new ArrayList<ChoreAction>(), 1000);
        boolean added = choreWatcher.addChore(chore);
        assertTrue(added);
        boolean finished = choreWatcher.finishChore("abc");
        assertTrue(finished);
    }

    @Test
    public void testRefuseDupChore() throws Exception {
        ChoreWatcher choreWatcher = new ChoreWatcher(dummyId, spawnDataStore, choreExecutor, 1000);
        Chore chore1 = new Chore("abc", null, new ArrayList<ChoreAction>(), new ArrayList<ChoreAction>(), 10);
        Chore chore2 = new Chore("abc", null, new ArrayList<ChoreAction>(), new ArrayList<ChoreAction>(), 10);
        choreWatcher.addChore(chore1);
        boolean added = choreWatcher.addChore(chore2);
        assertTrue(!added);
    }

    @Test
    public void testChoreExpire() throws Exception {
        ChoreWatcher choreWatcher = new ChoreWatcher(dummyId, spawnDataStore, choreExecutor, 1000);
        Chore chore = new Chore("abc", null, new ArrayList<ChoreAction>(), new ArrayList<ChoreAction>(), 10);
        choreWatcher.addChore(chore);
        assertTrue(choreWatcher.hasChore("abc"));
        Thread.sleep(1200);
        assertTrue("expected chore with key 'de' to be removed'", !choreWatcher.hasChore("de"));
    }

    @Test
    public void testChoreEncoding() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Chore chore = new Chore("abc", null, new ArrayList<ChoreAction>(), new ArrayList<ChoreAction>(), 1000);
        String encChore = mapper.writeValueAsString(chore);
        assertTrue(encChore.contains("startTime")
                   && encChore.contains("status")
                   && encChore.contains("maxRunTimeMillis"));
    }


    @Test
    public void testChoreEncoding_withPromotionAction() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<ChoreAction> actions = new ArrayList<ChoreAction>();
        actions.add(new PromoteAction(new Spawn(), new JobKey("1", 1), "targetHost", "sourceHost", false, true, false));
        Chore chore = new Chore("abc", null, new ArrayList<ChoreAction>(), actions, 1000);
        String encChore = mapper.writeValueAsString(chore);
        assertTrue(encChore.contains("startTime")
                   && encChore.contains("status")
                   && encChore.contains("maxRunTimeMillis"));

        Chore decodedChore = mapper.readValue(encChore, Chore.class);
        ArrayList<ChoreAction> decodedActions = decodedChore.getOnError();
        Assert.assertTrue(decodedActions.size() == 1);
        assertEquals("targetHost", ((PromoteAction) decodedActions.iterator().next()).getTargetHostUUID());
        assertEquals("sourceHost", ((PromoteAction) decodedActions.iterator().next()).getSourceHostUUID());
        assertEquals(new JobKey("1", 1), ((PromoteAction) decodedActions.iterator().next()).getTaskKey());
    }

    @Test
    public void testActionEncoding() throws Exception {
        ScheduledExecutorService executor = MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("ChoreWatcher-%d").build()));
        ObjectMapper mapper = new ObjectMapper();

        ChoreAction ca = new TestAction("abc");
        String encAction = mapper.writeValueAsString(ca);
        ChoreAction decAction = mapper.readValue(encAction, ChoreAction.class);
        assertEquals(decAction.getClass(), TestAction.class);

        ArrayList<ChoreAction> onFinish = new ArrayList<ChoreAction>();
        onFinish.add(new TestAction("123"));
        onFinish.add(new TestAction("456"));
        Chore chore = new Chore("some chore", null, onFinish, null, 4000);
        String encChore = mapper.writeValueAsString(chore);
        Chore decChore = mapper.readValue(encChore, Chore.class);
        decChore.doFinish(executor);
        Thread.sleep(200);
        for (ChoreAction action : decChore.getOnFinish()) {
            assertEquals(action.getClass(), TestAction.class);
            TestAction ta = (TestAction) action;
            assertTrue(ta.getCompleted());
        }
    }

    @Test
    public void testSpawnChoreWatcherEncoding() throws Exception {
        Spawn spawn = new Spawn();
        SpawnChoreWatcher scw = new SpawnChoreWatcher(spawn, dummyId, spawnDataStore, choreExecutor, 1000);
        ArrayList<ChoreAction> actions = new ArrayList<ChoreAction>();
        actions.add(new PromoteAction(spawn, new JobKey("1", 1), "targetHost", "sourceHost", false, true, false));
        ArrayList<ChoreCondition> conditions = new ArrayList<ChoreCondition>();
        JobKey condJobKey = new JobKey("aaa", 1);
        conditions.add(new IdleChoreCondition(spawn, condJobKey));
        Chore chore = new Chore("abc", conditions, new ArrayList<ChoreAction>(), actions, 1000);
        scw.addChore(chore);
        SpawnChoreWatcher scw2 = new SpawnChoreWatcher(spawn, dummyId, spawnDataStore, choreExecutor, 1000);
        assertTrue("new choreawtcher should have old chore", scw2.hasChore("abc"));
        Chore decodedChore = scw2.removeChore("abc");
        assertEquals("chore key should be accurate", decodedChore.getKey(), "abc");
        assertEquals("chore onError should be accurate", decodedChore.getOnError().size(), 1);
        ChoreAction action = decodedChore.getOnError().get(0);
        assertEquals("action class should be accurate", action.getClass(), PromoteAction.class);
        assertEquals("action parameters should be accurate", ((PromoteAction) action).getTargetHostUUID(), "targetHost");
        assertEquals(((PromoteAction) action).getTaskKey(), new JobKey("1", 1));
        assertTrue("chore conditions should be accurate", decodedChore.getConditions().size() == 1);
        ChoreCondition cond = decodedChore.getConditions().get(0);
        assertTrue("condition task key should be accurate", cond instanceof IdleChoreCondition &&
                                                            ((IdleChoreCondition) cond).getTaskKey().matches(condJobKey));
    }

    @Test
    public void testSpawnChoreTimerReset() throws Exception {
        Spawn spawn = new Spawn();
        SpawnChoreWatcher scw = new SpawnChoreWatcher(spawn, dummyId, spawnDataStore, choreExecutor, 1000);
        Chore chore = new Chore("abcd", null, null, null, -1);
        chore.setStartTime(0l);
        scw.addChore(chore);
        scw.onAfterLoadState();
        assertTrue("chore start time should be reset", chore.getStartTime() > 0);
    }

}

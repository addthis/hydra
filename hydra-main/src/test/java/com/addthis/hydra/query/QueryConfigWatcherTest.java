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
package com.addthis.hydra.query;

import com.addthis.basis.test.SlowTest;

import com.addthis.bark.ZkStartUtil;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.ZnodeJob;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.ZookeeperDataStore;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
public class QueryConfigWatcherTest extends ZkStartUtil {

    JobConfigManager jcm;
    SpawnDataStore spawnDataStore;
    QueryConfigWatcher qcw;

    @Before
    public void setUp() throws Exception {
        spawnDataStore = new ZookeeperDataStore(myZkClient);
        jcm = setUpSampleState();
        qcw = new QueryConfigWatcher(spawnDataStore);
    }

    public JobConfigManager setUpSampleState() throws Exception {
        JobConfigManager jcm = new JobConfigManager(spawnDataStore);
        IJob job = new ZnodeJob("j1");
        job.setQueryConfig(new JobQueryConfig(true, 0));
        jcm.addJob(job);
        IJob job2 = new ZnodeJob("j2");
        job2.setQueryConfig(new JobQueryConfig(false, 5));
        jcm.addJob(job2);
        return jcm;
    }

    @Test
    public void sampleInit() throws Exception {

        assertEquals(true, qcw.safeToQuery("j1"));
        assertEquals(false, qcw.safeToQuery("j2"));

        assertEquals(false, qcw.shouldTrace("j1"));
        assertEquals(true, qcw.shouldTrace("j2"));

        assertEquals(100, qcw.consecutiveFailureThreshold("j1"));
    }


    @Test
    public void sampleRemove() throws Exception {
        assertEquals(true, qcw.safeToQuery("j1"));
        assertEquals(false, qcw.safeToQuery("j2"));
        jcm.deleteJob("j1");
        qcw.invalidateQueryConfigCache();
        assertEquals(false, qcw.safeToQuery("j1")); // del == false
        assertEquals(false, qcw.safeToQuery("j2"));
    }


    @Test
    public void sampleAddition() throws Exception {
        JobConfigManager jcm = setUpSampleState();

        IJob job = new ZnodeJob("j3");
        job.setQueryConfig(new JobQueryConfig(true, 137));
        jcm.addJob(job);
        assertEquals(true, qcw.safeToQuery("j3"));
    }

    @Test
    public void sampleChangeConfig() throws Exception {
        IJob job = new ZnodeJob("j1");
        job.setQueryConfig(new JobQueryConfig(false, 10));
        jcm.updateJob(job);
        assertEquals(false, qcw.safeToQuery("j1"));
        assertEquals(true, qcw.shouldTrace("j1"));
    }

}
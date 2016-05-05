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

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.ZnodeJob;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.ZookeeperDataStore;
import com.addthis.hydra.query.spawndatastore.QueryConfigWatcher;
import com.addthis.hydra.util.ZkCodecStartUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class QueryConfigWatcherTest extends ZkCodecStartUtil {

    JobConfigManager jcm;
    SpawnDataStore spawnDataStore;
    QueryConfigWatcher qcw;

    @Before
    public void setUp() throws Exception {
        spawnDataStore = new ZookeeperDataStore(zkClient);
        jcm = setUpSampleState();
        qcw = new QueryConfigWatcher(spawnDataStore);
    }

    public JobConfigManager setUpSampleState() throws Exception {
        JobConfigManager jcm = new JobConfigManager(spawnDataStore, null);
        IJob job = new ZnodeJob("j1");
        job.setQueryConfig(new JobQueryConfig(true));
        jcm.addJob(job);
        IJob job2 = new ZnodeJob("j2");
        job2.setQueryConfig(new JobQueryConfig(false));
        jcm.addJob(job2);
        return jcm;
    }

    @Test
    public void sampleInit() throws Exception {
        assertTrue(qcw.safeToQuery("j1"));
        assertFalse(qcw.safeToQuery("j2"));
    }


    @Test
    public void sampleRemove() throws Exception {
        assertTrue(qcw.safeToQuery("j1"));
        assertFalse(qcw.safeToQuery("j2"));
        jcm.deleteJob("j1");
        qcw.invalidateQueryConfigCache();
        assertFalse(qcw.safeToQuery("j1")); // del == false
        assertFalse(qcw.safeToQuery("j2"));
    }


    @Test
    public void sampleAddition() throws Exception {
        JobConfigManager jcm = setUpSampleState();

        IJob job = new ZnodeJob("j3");
        job.setQueryConfig(new JobQueryConfig(true));
        jcm.addJob(job);
        assertTrue(qcw.safeToQuery("j3"));
    }

    @Test
    public void sampleChangeConfig() throws Exception {
        IJob job = new ZnodeJob("j1");
        job.setQueryConfig(new JobQueryConfig(false));
        jcm.updateJob(job);
        assertFalse(qcw.safeToQuery("j1"));
    }

}
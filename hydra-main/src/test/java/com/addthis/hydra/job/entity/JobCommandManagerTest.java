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
package com.addthis.hydra.job.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import com.addthis.hydra.job.Job;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

public class JobCommandManagerTest extends JobEntityManagerTestBase {

    private Job job;
    private JobCommandManager manager;

    @Before
    public void setUp() throws Exception {
        initSpawnMocks();
        // mock a job that uses "my-command" 
        job = new Job("my_mock_job", "bob");
        when(spawn.listJobs()).thenReturn(Lists.newArrayList(job));
        job.setCommand("my-command");
        
        manager = new JobCommandManager(spawn);
    }
    
    @Test
    public void dataStorePath() {
        assertEquals("/spawn/common/command", manager.getDataStorePath());
    }

    @Test
    public void findDependentJob_Found() throws Exception {
        assertEquals(job, manager.findDependentJob(spawn, "my-command"));
    }

    @Test
    public void findDependentJob_NotFound() throws Exception {
        assertNull(manager.findDependentJob(spawn, "her-command"));
    }

}

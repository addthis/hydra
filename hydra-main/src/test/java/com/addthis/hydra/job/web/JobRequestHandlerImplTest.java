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
package com.addthis.hydra.job.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.entity.JobCommandManager;
import com.addthis.hydra.job.spawn.Spawn;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

public class JobRequestHandlerImplTest {

    private Spawn spawn;
    private JobCommandManager jobCommandManager;
    private JobRequestHandlerImpl impl;
    private String username = "megatron";
    private KVPairs kv;

    @Before
    public void setUp() {
        // mocks and stubs
        spawn = mock(Spawn.class);
        jobCommandManager = mock(JobCommandManager.class);
        when(spawn.getJobCommandManager()).thenReturn(jobCommandManager);
        when(jobCommandManager.getEntity("default-task")).thenReturn(new JobCommand());

        impl = new JobRequestHandlerImpl(spawn);
        kv = new KVPairs();
    }

    @Test
    public void createJob() throws Exception {
        // stub spawn calls
        Job job = new Job("new_job_id", "megatron");
        when(spawn.createJob("megatron", -1, Collections.<String> emptyList(), "default", "default-task")).thenReturn(job);
        when(spawn.getJob("new_job_id")).thenReturn(job);

        kv.add("config", "my job config");
        kv.add("command", "default-task");
        assertSame("returned job", job, impl.createOrUpdateJob(kv, username));

        // verify spawn calls
        verify(spawn).updateJob(job);
        verify(spawn).setJobConfig("new_job_id", "my job config");
        verify(spawn).submitConfigUpdate("new_job_id", null);
    }

    @Test
    public void createJob_MissingCommand() throws Exception {
        kv.add("config", "my job config");
        callAndVerifyBadRequest();
        verifyNoSpawnCreateJobCall();
    }

    @Test
    public void createJob_InvalidCommand() throws Exception {
        kv.add("command", "bogus-task");
        kv.add("config", "my job config");
        callAndVerifyBadRequest();
        verifyNoSpawnCreateJobCall();
    }

    @Test
    public void createJob_MissingConfig() throws Exception {
        kv.add("command", "default-task");
        callAndVerifyBadRequest();
        verifyNoSpawnCreateJobCall();
    }

    @Test
    public void createJob_ConfigTooLarge() throws Exception {
        kv.add("command", "default-task");
        kv.add("config", Strings.repeat('x', 1_000_001));
        callAndVerifyBadRequest();
        verifyNoSpawnCreateJobCall();
    }

    @Test
    public void updateJob() throws Exception {
        // stub spawn calls
        Job job = new Job("existing_job_id", "megatron");
        job.setCommand("old-task");
        when(spawn.getJob("existing_job_id")).thenReturn(job);

        kv.add("id", "existing_job_id");
        kv.add("config", "my job config");
        kv.add("command", "default-task");
        assertSame("returned job", job, impl.createOrUpdateJob(kv, username));
        assertEquals("command is updated", "default-task", job.getCommand());

        verifyNoSpawnCreateJobCall();
        // verify other spawn calls
        verify(spawn).updateJob(job);
        verify(spawn).setJobConfig("existing_job_id", "my job config");
        verify(spawn).submitConfigUpdate("existing_job_id", null);
    }

    @Test
    public void updateJob_InvalidJobId() throws Exception {
        kv.add("id", "bogus_job_id");
        kv.add("config", "my job config");
        kv.add("command", "default-task");
        callAndVerifyBadRequest();

        verify(spawn, never()).updateJob(any(Job.class));
    }

    /** If command is not specified when updating a job, it should remain unchanged */
    @Test
    public void updateJob_MissingCommand() throws Exception {
        // stub spawn calls
        Job job = new Job("existing_job_id", "megatron");
        job.setCommand("old-task");
        when(spawn.getJob("existing_job_id")).thenReturn(job);

        kv.add("id", "existing_job_id");
        kv.add("config", "my job config");
        assertSame("returned job", job, impl.createOrUpdateJob(kv, username));
        assertEquals("command is unchanged", "old-task", job.getCommand());
    }

    /** If specified command is invalid when updating a job, job should remain unchanged */
    @Test
    public void updateJob_InvalidCommand() throws Exception {
        // stub spawn calls
        Job job = new Job("existing_job_id", "megatron");
        job.setCommand("old-task");
        when(spawn.getJob("existing_job_id")).thenReturn(job);

        kv.add("id", "existing_job_id");
        kv.add("config", "my job config");
        kv.add("command", "bogus-task");
        callAndVerifyBadRequest();

        assertEquals("command is unchanged", "old-task", job.getCommand());

        verify(spawn, never()).updateJob(job);
    }

    /** If config is not specified when updating a job, it should remain unchanged */
    @Test
    public void updateJob_MissingConfig() throws Exception {
        // stub spawn calls
        Job job = new Job("existing_job_id", "megatron");
        when(spawn.getJob("existing_job_id")).thenReturn(job);
        when(spawn.getJobConfig("existing_job_id")).thenReturn("old job config");

        kv.add("id", "existing_job_id");
        assertSame("returned job", job, impl.createOrUpdateJob(kv, username));

        verify(spawn, never()).setJobConfig(anyString(), anyString());
        verify(spawn, never()).submitConfigUpdate(anyString(), anyString());
    }

    /**
     * Only job parameters in the job config should be added/updated; those that are not should be 
     * removed/ignored
     */
    @Test
    public void updateJobParameters() throws Exception {
        // stub spawn calls
        Job job = new Job("existing_job_id", "megatron");
        job.setParameters(Lists.newArrayList(
                new JobParameter("start-date", "140908", null),
                new JobParameter("end-date", "140908", null)));
        when(spawn.getJob("existing_job_id")).thenReturn(job);

        kv.add("id", "existing_job_id");
        // new job config removes 'end-date' and adds 'foo'
        kv.add("config", "%[start-date]% %[foo]%");
        kv.add("command", "default-task");
        kv.add("sp_start-date", "140908");
        kv.add("sp_end-date", "140908"); // should be removed
        kv.add("sp_foo", "foo");
        kv.add("sp_bar", "bar"); // should be ignored
        assertSame("returned job", job, impl.createOrUpdateJob(kv, username));
        assertEquals("# of parameters", 2, job.getParameters().size());
        JobParameter foo = null;
        JobParameter startDate = null;
        for (JobParameter jp : job.getParameters()) {
            if ("start-date".equals(jp.getName())) {
                startDate = jp;
            } else if ("foo".equals(jp.getName())) {
                foo = jp;
            }
        }
        assertNotNull("parameter 'start-date' found", startDate);
        assertEquals("parameter 'start-date' value", "140908", startDate.getValue());
        assertNotNull("parameter 'foo' found", foo);
        assertEquals("parameter 'foo' value", "foo", foo.getValue());
    }

    private void verifyNoSpawnCreateJobCall() throws Exception {
        verify(spawn, never()).createJob(anyString(), anyInt(), anyCollectionOf(String.class), anyString(), anyString());
    }

    private void callAndVerifyBadRequest() throws Exception {
        try {
            impl.createOrUpdateJob(kv, username);
            fail("IllegalArgumentException expected but was not thrown");
        } catch (IllegalArgumentException e) {
            // GOOD!
        }
    }
}

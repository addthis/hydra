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
package com.addthis.hydra.job.web.resources;

import javax.ws.rs.core.Response;

import com.addthis.basis.kv.KVPairs;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.auth.PermissionsManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.JobRequestHandler;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class JobsResourceTest {

    private Spawn spawn;
    private JobRequestHandler requestHandler;
    private JobsResource resource;
    private KVPairs kv;

    @Before
    public void setUp() {
        // mocks and stubs
        spawn = mock(Spawn.class);
        when(spawn.getPermissionsManager()).thenReturn(PermissionsManager.createManagerAllowAll());
        requestHandler = mock(JobRequestHandler.class);

        resource = new JobsResource(spawn, requestHandler);
        kv = new KVPairs();
    }

    @Test
    public void saveJob() throws Exception {
        // stub spawn calls
        Job job = new Job("new_job_id", "megatron");
        when(requestHandler.createOrUpdateJob(kv, "megatron", "megatron", null, false)).thenReturn(job);
        Response response = resource.saveJob(kv, "megatron", "megatron", null, false);
        assertEquals(200, response.getStatus());
        verifyZeroInteractions(spawn);
    }

    @Test
    public void saveJob_BadParam() throws Exception {
        when(requestHandler.createOrUpdateJob(kv, "megatron", "megatron", null, false)).thenThrow(new IllegalArgumentException("bad param"));
        Response response = resource.saveJob(kv, "megatron", "megatron", null, false);
        assertEquals(400, response.getStatus());
        verifyZeroInteractions(spawn);
    }

    @Test
    public void saveJob_InternalError() throws Exception {
        when(requestHandler.createOrUpdateJob(kv, "megatron", "megatron", null, false)).thenThrow(new Exception("internal error"));
        Response response = resource.saveJob(kv, "megatron", "megatron", null, false);
        assertEquals(500, response.getStatus());
        verifyZeroInteractions(spawn);
    }

    @Test
    public void enableJob_safely() throws Exception {
        Job job1 = Configs.decodeObject(Job.class, "{id:job1, state:0, disabled:true}"); // idle
        Job job2 = Configs.decodeObject(Job.class, "{id:job2, state:5, disabled:true}"); // error
        when(spawn.getJob("job1")).thenReturn(job1);
        when(spawn.getJob("job2")).thenReturn(job2);

        Response response = resource.enableJob("job1,job2", "1", false, "megatron", "megatron", null);
        assertTrue("job1 should be enabled", job1.isEnabled());
        assertFalse("job2 should be disabled", job2.isEnabled());
        assertEquals("response status", 200, response.getStatus());
    }

    @Test
    public void enableJob_unsafely() throws Exception {
        Job job1 = Configs.decodeObject(Job.class, "{id:job1, state:0, disabled:true}"); // idle
        Job job2 = Configs.decodeObject(Job.class, "{id:job2, state:5, disabled:true}"); // error
        when(spawn.getJob("job1")).thenReturn(job1);
        when(spawn.getJob("job2")).thenReturn(job2);

        Response response = resource.enableJob("job1,job2", "1", true, "megatron", "megatron", null);
        assertTrue("job1 should be enabled", job1.isEnabled());
        assertTrue("job2 should be enabled", job2.isEnabled());
        assertEquals("response status", 200, response.getStatus());
    }
}

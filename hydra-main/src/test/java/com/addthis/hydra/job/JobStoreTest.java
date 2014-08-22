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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.addthis.basis.util.Files;

import com.addthis.hydra.job.store.JobStore;
import com.addthis.maljson.JSONArray;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JobStoreTest {

    private static final String JOB_ID = "abc123";

    private File tempDir;
    private JobStore jobStore;

    @Before
    public void setUp() throws Exception {
        System.setProperty("job.store.remote", "false");
        tempDir = Files.createTempDir();
        jobStore = new JobStore(tempDir);
    }

    @After
    public void tearDown() throws Exception {
        Files.deleteDir(tempDir);
    }

    @Test
    public void jobStoreTest() throws Exception {
        for (int i = 0; i < 10; i++) {
            jobStore.submitConfigUpdate(JOB_ID, "anon" + i, "jobconfig" + i, null);
        }
        String finalConfig = "a\nb\nc";
        jobStore.submitConfigUpdate(JOB_ID, "anonF", finalConfig, null);
        JSONArray arr = jobStore.getHistory(JOB_ID);
        assertEquals("should get correct number of updates", 11, arr.length());
        assertTrue("update should have correct changes", arr.getJSONObject(5).getString("msg").matches(".*added 1.*removed 1.*"));
        String midCommitId = arr.getJSONObject(5).getString("commit");
        assertEquals("should get correct historical config", "jobconfig5\n", jobStore.fetchHistoricalConfig("abc123", midCommitId));
        assertEquals("should still have correct latest config", finalConfig + "\n", new String(Files.read(new File(tempDir + "/jobs/" + JOB_ID))));
        String diff = jobStore.getDiff(JOB_ID, midCommitId);
        assertTrue("diff should have expected components", diff.contains("-jobconfig") && diff.contains("+a"));
    }

    @Test
    public void getDeletedJobConfigSuccessTest() throws Exception {
        jobStore.submitConfigUpdate(JOB_ID, "bob", "config 1", null);
        jobStore.submitConfigUpdate(JOB_ID, "bob", "config 2", null);
        jobStore.delete(JOB_ID);
        assertEquals("config 2\n", jobStore.getDeletedJobConfig(JOB_ID));
    }

    @Test
    public void getDeletedJobConfigNoCommitTest() throws Exception {
        jobStore.submitConfigUpdate(JOB_ID, "bob", "config 1", null);
        assertNull(jobStore.getDeletedJobConfig("bogus_job_id"));
    }

    @Test(expected = GitAPIException.class)
    public void getDeletedJobConfigGitErrorTest() throws Exception {
        // without the init commit jgit will throw an No HEAD exception 
        jobStore.getDeletedJobConfig("bogus_job_id");
    }
    
    @Test
    public void getHistoryNonExistentJobTest() throws Exception {
        jobStore.submitConfigUpdate(JOB_ID, "bob", "config 1", null);
        String history = jobStore.getHistory("bogus_job_id").toString();
        assertEquals("[]", history);
    }
}

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

import java.io.File;

import com.addthis.basis.util.Files;

import com.addthis.hydra.job.store.JobStore;
import com.addthis.maljson.JSONArray;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobStoreTest {

    @Test
    public void jobStoreTest() throws Exception {
        System.setProperty("job.store.remote", "false");
        File tempDir = Files.createTempDir();
        JobStore jobStore = new JobStore(tempDir);
        String jobId = "abc123";
        for (int i = 0; i < 10; i++) {
            jobStore.submitConfigUpdate(jobId, "anon" + i, "jobconfig" + i, null);
        }
        String finalConfig = "a\nb\nc";
        jobStore.submitConfigUpdate(jobId, "anonF", finalConfig, null);
        JSONArray arr = jobStore.getHistory(jobId);
        assertEquals("should get correct number of updates", 11, arr.length());
        assertTrue("update should have correct changes", arr.getJSONObject(5).getString("msg").matches(".*added 1.*removed 1.*"));
        String midCommitId = arr.getJSONObject(5).getString("commit");
        assertEquals("should get correct historical config", "jobconfig5\n", jobStore.fetchHistoricalConfig("abc123", midCommitId));
        assertEquals("should still have correct latest config", finalConfig + "\n", new String(Files.read(new File(tempDir + "/jobs/" + jobId))));
        String diff = jobStore.getDiff(jobId, midCommitId);
        assertTrue("diff should have expected components", diff.contains("-jobconfig") && diff.contains("+a"));
        Files.deleteDir(tempDir);
    }
}

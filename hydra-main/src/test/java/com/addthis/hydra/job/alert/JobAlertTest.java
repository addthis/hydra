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

package com.addthis.hydra.job.alert;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import java.util.List;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static com.addthis.codec.config.Configs.decodeObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobAlertTest {

    @Test
    public void basicTriggerTest() throws Exception {
        long now = System.currentTimeMillis();

        Job idleJob = createJobWithState(JobState.IDLE);
        Job errorJob = createJobWithState(JobState.ERROR);
        Job runningJob = createJobWithState(JobState.RUNNING);

        AbstractJobAlert errorAlert = decodeObject(AbstractJobAlert.class, "alertId = errorAlert, type = 0, jobIds = []");
        assertNotNull("Error alert should trigger with at least one error job",
                      errorAlert.alertActiveForJob(null, errorJob, null));
        assertNull("Error alert should not trigger with only idle job",
                   errorAlert.alertActiveForJob(null, idleJob, null));

        AbstractJobAlert completeAlert = decodeObject(AbstractJobAlert.class, "alertId = completeAlert, type = 1, jobIds = []");
        assertNull("Complete alert should not trigger with running job",
                   completeAlert.alertActiveForJob(null, runningJob, null));
        runningJob.setState(JobState.IDLE);
        assertNotNull("Complete alert should trigger on job completion",
                      completeAlert.alertActiveForJob(null, runningJob, null));
        runningJob.setState(JobState.RUNNING);

        AbstractJobAlert runtimeAlert = decodeObject(AbstractJobAlert.class,
                                                     "alertId = runtimeAlert, type = 2, timeout = 1 hour, jobIds = []");
        assertNull("Runtime alert should not trigger with idle job",
                   runtimeAlert.alertActiveForJob(null, idleJob, null));
        runningJob.setStartTime(now - 1000);
        assertNull("Runtime alert should not trigger with recently-submitted job",
                   runtimeAlert.alertActiveForJob(null, runningJob, null));
        runningJob.setStartTime(now - 180 * 60 * 1000);
        assertNotNull("Runtime alert should trigger with long-running job",
                      runtimeAlert.alertActiveForJob(null, runningJob, null));

        AbstractJobAlert rekickAlert = decodeObject(AbstractJobAlert.class,
                                                    "alertId = rekickAlert, type = 3, timeout = 1 hour, jobIds = []");
        idleJob.setEndTime(now - 10 * 60 * 1000);
        assertNull("Rekick alert should not fire after short time period",
                   rekickAlert.alertActiveForJob(null, idleJob, null));
        idleJob.setEndTime(now - 300 * 60 * 1000);
        assertNotNull("Rekick alert should fire after long time period",
                      rekickAlert.alertActiveForJob(null, idleJob, null));
    }

    @Test
    public void jsonTest() throws Exception {
        AbstractJobAlert initialAlert = decodeObject(AbstractJobAlert.class,
                                                     "alertId = sampleid, type = 0, email = \"someone@domain.com\", " +
                                                     "description = this is a new alert, jobIds = [j1, j2], " +
                                                     "webhookURL = \"http://example.com\"");
        JSONObject json = initialAlert.toJSON();
        assertEquals(initialAlert.alertId, json.getString("alertId"));
        assertEquals(0, json.getInt("type"));
        assertEquals(initialAlert.email, json.getString("email"));
        assertEquals(initialAlert.webhookURL, json.getString("webhookURL"));
        assertEquals(initialAlert.description, json.getString("description"));
        assertEquals(new JSONArray(initialAlert.jobIds), json.getJSONArray("jobIds"));
    }

    private Job createJobWithState(JobState jobState) throws Exception {
        Job job = new Job();
        job.setState(jobState);
        return job;
    }

    @Test
    public void constructingWebhookObject() throws Exception {
        Spawn mockSpawn = mock(Spawn.class);

        Job j1 = new Job("test_id");
        j1.setState(JobState.ERROR);
        j1.setDescription("job desc");
        j1.setStartTime(0L);
        j1.setEndTime(1L);

        JobTask task1 = new JobTask();
        task1.setByteCount(5);
        task1.setFileCount(4);
        task1.setState(JobTaskState.ERROR, true);

        JobTask task2 = new JobTask();
        task2.setByteCount(4);
        task2.setFileCount(6);
        task2.setState(JobTaskState.IDLE, true);

        JobTask task3 = new JobTask();
        task2.setByteCount(4);
        task2.setFileCount(6);
        task2.setState(JobTaskState.BUSY, true);

        j1.addTask(task1);
        j1.addTask(task2);
        j1.addTask(task3);

        when(mockSpawn.getJob("test_id")).thenReturn(j1);

        AbstractJobAlert runtimeAlert = decodeObject(AbstractJobAlert.class,
                                                     "alertId = a, type = 2, email = \"someone@domain.com\", " +
                                                     "description = runtime alert, jobIds = [j1], timeout = 1000");

        JobAlertRunner.AlertWebhookRequest obj =
                JobAlertRunner.getWebhookObject(mockSpawn, runtimeAlert, "http://localhost", "bad reason",
                                                ImmutableMap.of("test_id", "something horrible happened"));

        assertEquals("http://localhost", obj.getAlertLink());
        assertEquals("Task runtime exceeded", obj.getAlertType());
        assertEquals("bad reason", obj.getAlertReason());
        assertEquals("runtime alert", obj.getAlertDescription());
        List<JobAlertRunner.JobError> jobsInError = obj.getJobsInError();
        assertEquals(1, jobsInError.size());

        JobAlertRunner.JobError jobInfo = jobsInError.get(0);
        assertEquals("ERROR", jobInfo.getJobState().name());
        assertEquals("job desc", jobInfo.getDescription());
        assertEquals(0L, jobInfo.getStartTime());
        assertEquals(1L, jobInfo.getEndTime());
        assertEquals("test_id", jobInfo.getId());
        assertEquals("something horrible happened", jobInfo.getError());
        assertEquals("localhost", jobInfo.getClusterHead());
        assertEquals(3, jobInfo.getNodeCount());
        assertEquals(1, jobInfo.getErrorCount());
    }

    /** Error message should stay unchanged on repeated scan of triggered runtime or rekick exceeded alert */
    @Test
    public void noErrorChangeOnTriggeredTimeoutAlertRescan() throws Exception {
        AbstractJobAlert runtimeAlert = decodeObject(AbstractJobAlert.class,
                                                     "alertId = a, type = 2, email = \"someone@domain.com\", " +
                                                     "description = runtime alert, jobIds = [j1], timeout = 1000");
        AbstractJobAlert rekickAlert = decodeObject(AbstractJobAlert.class,
                                                    "alertId = b, type = 3, email = \"someone@domain.com\", " +
                                                    "description = rekick alert, jobIds = [j1], timeout = 1000");
        Job j1 = createJobWithState(JobState.RUNNING);
        j1.setStartTime(0L);
        j1.setEndTime(1L);
        String runtimeAlertMsg1 = runtimeAlert.alertActiveForJob(null, j1, null);
        String rekickAlertMsg1 = runtimeAlert.alertActiveForJob(null, j1, null);
        Thread.sleep(10);
        String runtimeAlertMsg2 = runtimeAlert.alertActiveForJob(null, j1, null);
        String rekickAlertMsg2 = runtimeAlert.alertActiveForJob(null, j1, null);
        assertEquals("runtime alert message unchanged", runtimeAlertMsg1, runtimeAlertMsg2);
        assertEquals("rekick alert message unchanged", rekickAlertMsg1, rekickAlertMsg2);
    }

    @Test
    public void conditionallyTriggerAlertOnCanaryException() throws Exception {
        AbstractJobAlert alert = decodeObject(AbstractJobAlert.class, "alertId = a, type = 5, description = canary alert, jobIds = []");
        Exception definitelyBadException = new RuntimeException("error");
        Exception normallyOkException = new RuntimeException(new SocketTimeoutException("socket timeout"));

        assertEquals("bad exception", definitelyBadException.toString(), alert.handleCanaryException(definitelyBadException, null));
        for (int i = 1; i < AbstractJobAlert.MAX_CONSECUTIVE_CANARY_EXCEPTION; i++) {
            assertNull("benign exception #" + i, alert.handleCanaryException(normallyOkException, null));
        }
        assertNotNull("benign exception (exceeds MAX_CONSECUTIVE_CANARY_EXCEPTION)", alert.handleCanaryException(normallyOkException, null));
        assertNull("benign exception (consective ex counter reset)", alert.handleCanaryException(normallyOkException, null));
    }

    @Test
    public void preserveExistingAlertOnCanaryException() throws Exception {
        AbstractJobAlert alert = decodeObject(AbstractJobAlert.class, "alertId = a, type = 5, description = canary alert, jobIds = []");
        Exception definitelyBadException = new RuntimeException("error");
        Exception normallyOkException = new RuntimeException(new SocketTimeoutException("socket timeout"));

        assertEquals("bad exception", definitelyBadException.toString(), alert.handleCanaryException(definitelyBadException, null));
        assertEquals("benign exception #1", "some previous error",
                     alert.handleCanaryException(normallyOkException, "some previous error"));
        assertNull("benign exception #2", alert.handleCanaryException(normallyOkException, null));
    }

    @Test
    public void queryDownAlertOnCanaryException() throws Exception {
        AbstractJobAlert alert = decodeObject(AbstractJobAlert.class, "alertId = a, type = 5, description = canary alert, jobIds = []");
        Exception e = new RuntimeException(new ConnectException());
        assertEquals("alert error message should be unchanged on ConnectException",
                     "previous error", alert.handleCanaryException(e, "previous error"));
    }
}

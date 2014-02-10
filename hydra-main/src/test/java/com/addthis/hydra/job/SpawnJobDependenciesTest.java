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

import java.io.IOException;

import com.addthis.hydra.common.test.LocalStackTest;
import com.addthis.hydra.task.util.HTTPRequest;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LocalStackTest.class)
public class SpawnJobDependenciesTest {

    @Test
    public void testDependenciesJobCreation() {
        String jobId = null;
        try {
            // create an empty job named "job1"
            String creationString = createJob("job1", "");
            JSONObject creation = new JSONObject(creationString);
            assertTrue(creation.has("id"));
            jobId = creation.getString("id");
            String dependenciesString = getDependencies();
            JSONObject dependencies = new JSONObject(dependenciesString);
            assertTrue(dependencies.has("nodes"));
            assertTrue(dependencies.has("edges"));
            JSONArray nodes = dependencies.getJSONArray("nodes");
            JSONObject node = null;
            for (int i = 0; i < nodes.length(); i++) {
                JSONObject current = nodes.getJSONObject(i);
                assertTrue(current.has("id"));
                if (current.getString("id").equals(jobId)) {
                    node = current;
                    break;
                }
            }
            assertNotNull(node);
            // Ensure that there are no edges to this new job
            JSONArray edges = dependencies.getJSONArray("edges");
            for (int i = 0; i < edges.length(); i++) {
                JSONObject current = edges.getJSONObject(i);
                assertTrue(current.has("source"));
                assertTrue(current.has("sink"));

                if (current.getString("source").equals(jobId) ||
                    current.getString("sink").equals(jobId)) {
                    fail("Illegal edge found from " +
                         current.getString("source") + " to " + current.getString("sink"));
                }
            }
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            try {
                if (jobId != null) {
                    deleteJob(jobId);
                }
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }

    @Test
    public void testDependenciesJobDeletion() {
        String jobId = null;
        try {
            // create an empty job named "job1"
            String creationString = createJob("job1", "");
            JSONObject creation = new JSONObject(creationString);
            assertTrue(creation.has("id"));
            jobId = creation.getString("id");
            // delete the job named "job1"
            deleteJob(jobId);
            jobId = null;
            String dependenciesString = getDependencies();
            JSONObject dependencies = new JSONObject(dependenciesString);
            assertTrue(dependencies.has("nodes"));
            assertTrue(dependencies.has("edges"));
            JSONArray nodes = dependencies.getJSONArray("nodes");
            JSONObject node = null;
            for (int i = 0; i < nodes.length(); i++) {
                JSONObject current = nodes.getJSONObject(i);
                assertTrue(current.has("id"));
                if (current.getString("id").equals(jobId)) {
                    node = current;
                    break;
                }
            }
            assertNull(node);
            // Ensure that there are no edges to the deleted job
            JSONArray edges = dependencies.getJSONArray("edges");
            for (int i = 0; i < edges.length(); i++) {
                JSONObject current = edges.getJSONObject(i);
                assertTrue(current.has("source"));
                assertTrue(current.has("sink"));

                if (current.getString("source").equals(jobId) ||
                    current.getString("sink").equals(jobId)) {
                    fail("Illegal edge found from " +
                         current.getString("source") + " to " + current.getString("sink"));
                }
            }

        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            try {
                if (jobId != null) {
                    deleteJob(jobId);
                }
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }

    @Test
    public void testCreateDependency() {
        String jobId1 = null, jobId2 = null;
        try {
            // create an empty job named "job1"
            String creationString1 = createJob("job1", "");
            JSONObject creation1 = new JSONObject(creationString1);
            assertTrue(creation1.has("id"));
            jobId1 = creation1.getString("id");

            // create a job named "job2" with a parameter that references "job1"
            String creationString2 = createJob("job2", "%25%5Bjobref%3A" + jobId1 + "%5D%25");
            JSONObject creation2 = new JSONObject(creationString2);
            assertTrue(creation2.has("id"));
            jobId2 = creation2.getString("id");

            String dependenciesString = getDependencies();
            JSONObject dependencies = new JSONObject(dependenciesString);

            assertTrue(dependencies.has("nodes"));
            assertTrue(dependencies.has("edges"));

            JSONArray nodes = dependencies.getJSONArray("nodes");
            JSONObject node1 = null, node2 = null;
            for (int i = 0; i < nodes.length() && (node1 == null || node2 == null); i++) {
                JSONObject current = nodes.getJSONObject(i);
                assertTrue(current.has("id"));
                if (current.getString("id").equals(jobId1)) {
                    node1 = current;
                } else if (current.getString("id").equals(jobId2)) {
                    node2 = current;
                }
            }
            assertNotNull(node1);
            assertNotNull(node2);

            JSONArray edges = dependencies.getJSONArray("edges");
            JSONObject edge = null;
            for (int i = 0; i < edges.length(); i++) {
                JSONObject current = edges.getJSONObject(i);
                assertTrue(current.has("source"));
                assertTrue(current.has("sink"));

                if (current.getString("source").equals(jobId1) &&
                    current.getString("sink").equals(jobId2)) {
                    edge = current;
                } else if (current.getString("source").equals(jobId1) &&
                           !current.getString("sink").equals(jobId2)) {
                    fail("Illegal edge found from " +
                         current.getString("source") + " to " + current.getString("sink"));
                } else if (!current.getString("source").equals(jobId1) &&
                           current.getString("sink").equals(jobId2)) {
                    fail("Illegal edge found from " +
                         current.getString("source") + " to " + current.getString("sink"));
                }
            }
            assertNotNull(edge);
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            try {
                if (jobId1 != null) {
                    deleteJob(jobId1);
                }
                if (jobId2 != null) {
                    deleteJob(jobId2);
                }
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }

	/* Helper methods */

    public static String createJob(String description, String config) throws IOException {
        return HTTPRequest.sendHTTPPost("http://localhost:5052/job/submit",
                "nodes=1&description=" + description + "&config=" + config +
                "&maxRunTime=&rekickTimeout=&priority=0&command=default-task" +
                "&hourlyBackups=1&dailyBackups=1&weeklyBackups=1&monthlyBackups=1" +
                "&minionType=default&replicas=1&readOnlyReplicas=0&dontAutoBalanceMe=true" +
                "&dontDeleteMe=false&maxSimulRunning=0&alerts=%5B%5D&spawn=0&manual=1&qc_canQuery=true" +
                "&qc_consecutiveFailureThreshold=&qc_queryTraceLevel=",
                "hydra");
    }

    public static String getDependencies() throws IOException {
        return HTTPRequest.sendHTTPGet("http://localhost:5052/job/dependencies/all");
    }

    public static String deleteJob(String jobId) throws IOException {
        return HTTPRequest.sendHTTPGet("http://localhost:5052/job/delete?id=" + jobId);
    }

}

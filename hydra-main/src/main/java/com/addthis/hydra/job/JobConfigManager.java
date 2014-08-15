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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.spawn.JobAlert;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_JOB_CONFIG_PATH;

/**
 * Stuff about jobs that *others* care about, not in the giant Spawnstate ball of mud.
 * We assume that only Spawn updates the job znodes, and only through this class.
 */
// {queryconfig,config,jobtask/[n]} under job uuid

// Can't use ZkMessageProducer because this involves multiple znodes
public class JobConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(JobConfigManager.class);

    private static final Codec codec = CodecJSON.INSTANCE;

    private SpawnDataStore spawnDataStore;

    /* metrics */
    private final Histogram jobSizePersistHisto     =
            Metrics.newHistogram(JobConfigManager.class, "jobSizePersistHisto");
    private final Histogram jobTaskSizePersistHisto =
            Metrics.newHistogram(JobConfigManager.class, "jobTaskSizePersistHisto");
    private final Timer     addJobTimer             =
            Metrics.newTimer(JobConfigManager.class, "addJobTimer");
    private final Timer     updateJobTimer          =
            Metrics.newTimer(JobConfigManager.class, "updateJobTimer");

    private static final int loadThreads  = Parameter.intValue("job.config.load.threads", 8);
    private static final int jobChunkSize = Parameter.intValue("job.config.chunk.size", 30);

    private static final String configChildName      = "/config";
    private static final String queryConfigChildName = "/queryconfig";
    private static final String alertChildName       = "/alerts";
    private static final String tasksChildName       = "/tasks";
    private static final String brokerInfoChildName  = "/brokerinfo";
    private static final String taskChildName        = "/task";

    public JobConfigManager(SpawnDataStore spawnDataStore) {
        this.spawnDataStore = spawnDataStore;
    }

    public void writeUpdateIfDataNotNull(String path, String data) throws Exception {
        if (data == null) {
            logger.warn("Was going to update znode {} but data was null", new Object[]{path});
        } else {
            spawnDataStore.put(path, data);
        }
    }

    // todo: fail if already present?
    public void addJob(IJob ijob) throws Exception {
        TimerContext addJobTimerContext = addJobTimer.time();
        ZnodeJob job = new ZnodeJob(ijob);
        String jobPath = getJobPath(ijob.getId());
        try {
            final String jobCodec = new String(codec.encode(job.getRootData()));
            jobSizePersistHisto.update(jobCodec.length());
            spawnDataStore.putAsChild(SPAWN_JOB_CONFIG_PATH, job.getId(), jobCodec);
            writeUpdateIfDataNotNull(jobPath + configChildName,
                                     job.getConfig() == null ? "" : job.getConfig());
            writeUpdateIfDataNotNull(jobPath + queryConfigChildName,
                                     job.getQueryConfig() == null ?
                                     "" :
                                     new String(codec.encode(job.getQueryConfig())));
            // this is just a marker so that we know to use the 'new' configuration
            spawnDataStore.put(jobPath + tasksChildName, "");
        } finally {
            addJobTimerContext.stop();
        }
    }

    public void updateJob(IJob ijob) {
        this.updateJob(ijob, null);
    }

    public void updateJob(IJob ijob, JobTask changedTask) {
        TimerContext updateJobTimerContext = updateJobTimer.time();
        ZnodeJob job = new ZnodeJob(ijob);
        String jobPath = getJobPath(ijob.getId());
        // for transition, it's possible an already existing job does
        // not have a znode, (There should probably be a zkutils for
        // writedata and make sure it exists.
        try {
            final String jobCodec = new String(codec.encode(job.getRootData()));
            jobSizePersistHisto.update(jobCodec.length());
            spawnDataStore.putAsChild(SPAWN_JOB_CONFIG_PATH, job.getId(), jobCodec);
            writeUpdateIfDataNotNull(jobPath + alertChildName, new String(codec.encode(job.getAlerts())));
            writeUpdateIfDataNotNull(jobPath + queryConfigChildName, new String(codec.encode(job.getQueryConfig())));
            // this is just a marker so that we know to use the 'new' configuration
            spawnDataStore.put(jobPath + tasksChildName, "");
        } catch (Exception e) {
            logger.warn("Failing to update job, bailing", e);
            throw new RuntimeException(e);
        } finally {
            updateJobTimerContext.stop();
        }
    }

    /**
     * Internal function to load job alerts if any are set
     *
     * @param alertData The raw alert config string loaded from the SpawnDataStore
     * @return A List of JobAlerts decoded from the config
     */
    private List<JobAlert> loadAlerts(String alertData) {
        if (alertData != null) {
            try {
                return Jackson.defaultMapper().readValue(alertData, new TypeReference<List<JobAlert>>() {});
            } catch (Exception ex) {
                return null;
            }
        }
        return null;
    }

    /**
     * Use query data fetched from a SpawnDataStore to create the job object
     *
     * @param jobId     The jobId in question
     * @param queryData Query data describing the job configuration. Data for other jobs will be ignored.
     * @return The reconstituted job object
     * @throws Exception
     */
    private IJob createJobFromQueryData(String jobId, Map<String, String> queryData) throws Exception {
        String jobPath = getJobPath(jobId);
        String rstring = spawnDataStore.getChild(SPAWN_JOB_CONFIG_PATH, jobId);
        if (rstring == null) {
            return null;
        }
        ZnodeJob.RootZnodeData rznd = codec.decode(ZnodeJob.RootZnodeData.class, rstring.getBytes());
        String config = queryData.get(jobPath + configChildName);
        String queryConfigString = queryData.get(jobPath + queryConfigChildName);
        JobQueryConfig jqc = codec.decode(JobQueryConfig.class, queryConfigString.getBytes());
        String jobAlertsConfig = queryData.get(jobPath + alertChildName);
        List<JobAlert> alerts = loadAlerts(jobAlertsConfig);
        // Make sure job config path exists
        spawnDataStore.put(SPAWN_JOB_CONFIG_PATH, "");
        String tasksData = queryData.get(jobPath + tasksChildName);
        if (tasksData != null) {
            // load from new config where task data is stored on root node
            return new ZnodeJob(rznd, config, jqc, alerts);
        } else {
            String taskData = queryData.get(jobPath + taskChildName);
            if (taskData != null) {
                // old style, will be removed in future versions
                return loadLegacyTaskData(jobPath, rznd, config, jqc, alerts);
            }
        }
        logger.info("No tasks available for path: " + jobPath);
        return new ZnodeJob(rznd, config, jqc, alerts);
    }

    public IJob getJob(String jobId) {
        try {
            Map<String, String> queryData = fetchJobData(jobId);
            if (queryData == null) {
                return null;
            }
            return createJobFromQueryData(jobId, queryData);
        } catch (Exception e) {
            logger.error("Failure creating job: " + jobId, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal function to fetch the job config, query config, etc. for a group of jobs using a single SpawnDataStore operation.
     *
     * @param jobId The jobId to fetch
     * @return A map describing the results of querying all the relevant paths
     */
    private Map<String, String> fetchJobData(String jobId) {
        String jobPath = getJobPath(jobId);
        String[] queryPaths = new String[] {jobPath, jobPath + configChildName, jobPath + queryConfigChildName, jobPath + alertChildName, jobPath + tasksChildName, jobPath + taskChildName};
        return spawnDataStore.get(queryPaths);
    }

    private IJob loadLegacyTaskData(String jobPath, ZnodeJob.RootZnodeData rznd, String config, JobQueryConfig jqc, List<JobAlert> alerts) throws Exception {
        List<JobTask> tasks = new ArrayList<>();
        List<String> children = (spawnDataStore.getChildrenNames(jobPath + taskChildName));
        Collections.sort(children);
        for (String taskId : children) {
            String taskString = spawnDataStore.get(jobPath + taskChildName + "/" + taskId);
            JobTask task = codec.decode(JobTask.class, taskString.getBytes());
            tasks.add(task);
        }
        return new ZnodeJob(rznd, config, jqc, alerts, tasks);
    }

    /**
     * Load the data for a series of job ids using a single query, then create each job using the queried data
     *
     * @param jobIdChunk The list of job ids to load
     * @return A map of the form {jobId : job}
     * @throws Exception
     */
    private Map<String, IJob> loadJobChunk(List<String> jobIdChunk) throws Exception {
        Map<String, IJob> rv = new HashMap<>();
        for (String jobId : jobIdChunk) {
            try {
                rv.put(jobId, createJobFromQueryData(jobId, fetchJobData(jobId)));
            } catch (Exception ex) {
                logger.error("Failed while reconstituting job " + jobId);
                logger.trace("Failed while reconstituting job ", ex);
            }
        }
        return rv;
    }

    /**
     * Find all job ids in the SpawnDataStore, split them into chunks, and then load the jobs from each chunk in parallel
     *
     * @return A map of all jobs found in the SpawnDataStore
     */
    public Map<String, IJob> getJobs() {
        final Map<String, IJob> jobs = new HashMap<>();
        List<String> jobNodes = spawnDataStore.getChildrenNames(SPAWN_JOB_CONFIG_PATH);
        if (jobNodes != null) {
            // Use multiple threads to query the database, and gather the results together
            ExecutorService executorService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(loadThreads, loadThreads, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()));
            for (List<String> jobIdChunk : Lists.partition(jobNodes, jobChunkSize)) {
                executorService.submit(new MapChunkLoader(jobs, jobIdChunk));
            }
            try {
                executorService.shutdown();
                executorService.awaitTermination(600000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for job import: ", e);
            }
        }
        return jobs;
    }

    public String getConfig(String jobUUID) {
        return spawnDataStore.get(getJobPath(jobUUID) + configChildName);
    }

    public void setConfig(String jobId, String config) throws Exception {
        if (jobId != null && config != null) {
            spawnDataStore.put(getJobPath(jobId) + configChildName, config);
        }
    }

    /**
     * Internal class to fetch a chunk of jobIds, then push the results into a master map
     */
    private class MapChunkLoader implements Runnable {

        private final Map<String, IJob> jobs;
        private final List<String> chunk;

        public MapChunkLoader(Map<String, IJob> jobs, List<String> chunk) {
            this.jobs = jobs;
            this.chunk = chunk;
        }

        @Override
        public void run() {
            try {
                Map<String, IJob> chunkMap = loadJobChunk(chunk);
                synchronized (jobs) {
                    jobs.putAll(chunkMap);
                }
            } catch (Exception e) {
                logger.error("While getting all jobs, error getting: " + chunk, e);
                throw new RuntimeException(e);
            }
        }
    }

    public void deleteJob(String jobUUID) {
        try {
            String jobPath = getJobPath(jobUUID);
            for (String subnode : Arrays.asList(alertChildName, queryConfigChildName, taskChildName, tasksChildName, configChildName, brokerInfoChildName)) {
                spawnDataStore.delete(jobPath + subnode);
            }
            spawnDataStore.delete(jobPath);
            spawnDataStore.deleteChild(SPAWN_JOB_CONFIG_PATH, jobUUID);
        } catch (Exception e) {
            logger.warn("Failing to delete job, bailing", e);
            throw new RuntimeException(e);
        }
    }

    private String getJobPath(String jobId) {
        return SPAWN_JOB_CONFIG_PATH + "/" + jobId;
    }

}

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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.annotations.Scaling;
import com.addthis.basis.util.Parameter;

import com.addthis.basis.util.TokenReplacerOverflowException;
import com.addthis.codec.Codec;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.spawn.search.ExpandedConfigCacheSettings;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.basis.annotations.Scaling.Scale.SETUP;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_JOB_CONFIG_PATH;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Stuff about jobs that *others* care about, not in the giant Spawnstate ball of mud.
 * We assume that only Spawn updates the job znodes, and only through this class.
 */
// {queryconfig,config,jobtask/[n]} under job uuid

// Can't use ZkMessageProducer because this involves multiple znodes
public class JobConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(JobConfigManager.class);

    private static final Codec codec = CodecJSON.INSTANCE;

    /* metrics */
    private static final Histogram jobSizePersistHisto =
            Metrics.newHistogram(JobConfigManager.class, "jobSizePersistHisto");
    private static final Timer addJobTimer = Metrics.newTimer(JobConfigManager.class, "addJobTimer");
    private static final Timer updateJobTimer = Metrics.newTimer(JobConfigManager.class, "updateJobTimer");

    private static final int loadThreads  = Parameter.intValue("job.config.load.threads", 8);
    private static final int jobChunkSize = Parameter.intValue("job.config.chunk.size", 30);

    private static final String configChildName      = "/config";
    private static final String queryConfigChildName = "/queryconfig";
    private static final String alertChildName       = "/alerts";
    private static final String tasksChildName       = "/tasks";
    private static final String brokerInfoChildName  = "/brokerinfo";
    private static final String taskChildName        = "/task";

    @Nonnull private final SpawnDataStore spawnDataStore;
    @Nullable private final LoadingCache<String, String> expandedConfigCache;
    @Nullable private final JobExpander jobExpander;

    public JobConfigManager(SpawnDataStore spawnDataStore) {
        this.jobExpander = null;
        this.expandedConfigCache = null;
        this.spawnDataStore = spawnDataStore;
    }

    public JobConfigManager(SpawnDataStore spawnDataStore, JobExpander jobExpander, ExpandedConfigCacheSettings cacheSettings) {
        this.spawnDataStore = spawnDataStore;
        this.jobExpander = jobExpander;
        this.expandedConfigCache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSettings.maxSizeBytes)
                .weigher((String key, String value) -> {
                    return key.length() + value.length();
                })
                .expireAfterWrite(cacheSettings.maxAgeSeconds, TimeUnit.SECONDS)
                .build(new CacheLoader<String, String>() {

                    @Override
                    public String load(String jobId) throws Exception {
                        IJob job = getJob(jobId);
                        checkArgument(job != null, "no job exists with id " + jobId);
                        String config = getConfig(jobId);
                        if (config == null) {
                            return "";
                        }

                        return jobExpander.expandJob(config, job.getParameters());
                    }
                });
    }


    /**
     * Returns the cached config associated with jobUUID, if there is one, or null otherwise
     * @param jobUUID
     * @return expanded job configuration
     */
    @Nullable
    public String getCachedExpandedJob(String jobUUID) {
        return expandedConfigCache.getIfPresent(jobUUID);
    }

    /**
     * Generate or read an expanded config, keyed on job UID, in the cache.
     * @param jobUUID
     * @return expanded job configuration
     * @throws ExecutionException
     */
    public String getExpandedConfig(String jobUUID) throws ExecutionException {
        IJob job = getJob(jobUUID) ;
        checkArgument(job != null, "job with uid " + jobUUID + " does not exist");
        return this.expandedConfigCache.get(jobUUID);
    }

    /**
     * Generate or read an expanded config, keyed on job UID, in the cache. Bypasses the cache if either:
     *  1) the job has been modified since the request was made (using lastModifiedAt)
     *  2) a null jobUUID is provided
     *  3) a jobUUID which doesn't belong to a job is provided
     * @param jobUUID
     * @param rawConfig - the config to expand. Possibly different from the one owned by Job #jobUUID
     * @param parameters - the params to expand the config with. Possibly different from the ones owned by Job #jobUUID
     * @param lastModifiedAt - at the time this request is made, the lastModifiedAt timestamp of the job if one exists
     * @return the expanded job config
     * @throws ExecutionException
     * @throws TokenReplacerOverflowException
     */
    @Nullable public String getExpandedConfig(@Nullable String jobUUID, String rawConfig, Collection<JobParameter> parameters, long lastModifiedAt) throws ExecutionException, FailedJobExpansionException {
        IJob job = getJob(jobUUID);

        if (job == null || job.lastModifiedAt() != lastModifiedAt) {
            return jobExpander.expandJob(rawConfig, parameters);
        }
        else {
            return expandedConfigCache.get(jobUUID);
        }
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
            if (job.getConfig() == null) {
                writeUpdateIfDataNotNull(jobPath + configChildName, "");
            } else {
                writeUpdateIfDataNotNull(jobPath + configChildName, job.getConfig());
            }
            if (job.getQueryConfig() == null) {
                writeUpdateIfDataNotNull(jobPath + queryConfigChildName, "");
            } else {
                writeUpdateIfDataNotNull(jobPath + queryConfigChildName,
                                         new String(codec.encode(job.getQueryConfig())));
            }
            // this is just a marker so that we know to use the 'new' configuration
            spawnDataStore.put(jobPath + tasksChildName, "");
        } finally {
            addJobTimerContext.stop();
        }
    }

    public void updateJob(IJob ijob) {
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
            expandedConfigCache.invalidate(job.getId());
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
     * Use query data fetched from a SpawnDataStore to create the job object
     *
     * @param jobId     The jobId in question
     * @param queryData Query data describing the job configuration. Data for other jobs will be ignored.
     * @return The reconstituted job object
     * @throws Exception
     */
    @Nullable private IJob createJobFromQueryData(String jobId, Map<String, String> queryData) throws Exception {
        String jobPath = getJobPath(jobId);
        String rstring = spawnDataStore.getChild(SPAWN_JOB_CONFIG_PATH, jobId);
        if (rstring == null) {
            return null;
        }
        ZnodeJob.RootZnodeData rznd = codec.decode(ZnodeJob.RootZnodeData.class, rstring.getBytes());
        String config = queryData.get(jobPath + configChildName);
        String queryConfigString = queryData.get(jobPath + queryConfigChildName);
        JobQueryConfig jqc = codec.decode(JobQueryConfig.class, queryConfigString.getBytes());
        // Make sure job config path exists
        spawnDataStore.put(SPAWN_JOB_CONFIG_PATH, "");
        String tasksData = queryData.get(jobPath + tasksChildName);
        if (tasksData != null) {
            // load from new config where task data is stored on root node
            return new ZnodeJob(rznd, config, jqc);
        } else {
            String taskData = queryData.get(jobPath + taskChildName);
            if (taskData != null) {
                // old style, will be removed in future versions
                return loadLegacyTaskData(jobPath, rznd, config, jqc);
            }
        }
        logger.info("No tasks available for path: {}", jobPath);
        return new ZnodeJob(rznd, config, jqc);
    }

    public IJob getJob(String jobId) {
        try {
            Map<String, String> queryData = fetchJobData(jobId);
            if (queryData == null) {
                return null;
            }
            return createJobFromQueryData(jobId, queryData);
        } catch (Exception e) {
            logger.error("Failure creating job: {}", jobId, e);
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
        String[] queryPaths =
                {jobPath, jobPath + configChildName, jobPath + queryConfigChildName, jobPath + alertChildName,
                 jobPath + tasksChildName, jobPath + taskChildName};
        return spawnDataStore.get(queryPaths);
    }

    private IJob loadLegacyTaskData(String jobPath,
                                    ZnodeJob.RootZnodeData rznd,
                                    String config,
                                    JobQueryConfig jqc) throws Exception {
        List<JobTask> tasks = new ArrayList<>();
        List<String> children = spawnDataStore.getChildrenNames(jobPath + taskChildName);
        Collections.sort(children);
        for (String taskId : children) {
            String taskString = spawnDataStore.get(jobPath + taskChildName + "/" + taskId);
            JobTask task = codec.decode(JobTask.class, taskString.getBytes());
            tasks.add(task);
        }
        return new ZnodeJob(rznd, config, jqc, tasks);
    }

    public String getConfig(String jobUUID) {
        return spawnDataStore.get(getJobPath(jobUUID) + configChildName);
    }

    public void setConfig(String jobId, String config) throws Exception {
        if (jobId != null && config != null) {
            spawnDataStore.put(getJobPath(jobId) + configChildName, config);
            expandedConfigCache.invalidate(jobId);
        }
    }

    /**
     * Find all job ids in the SpawnDataStore, split them into chunks, and then load the jobs from each chunk in parallel
     *
     * @return A map of all jobs found in the SpawnDataStore
     */
    @Scaling(SETUP)
    public Map<String, IJob> loadJobs() {
        final Map<String, IJob> jobs = new HashMap<>();
        List<String> jobNodes = spawnDataStore.getChildrenNames(SPAWN_JOB_CONFIG_PATH);
        if (jobNodes != null) {
            logger.info("Using {} threads to pull data on {} jobs", loadThreads, jobNodes.size());
            // Use multiple threads to query the database, and gather the results together
            ExecutorService executorService = new ThreadPoolExecutor(
                    loadThreads, loadThreads, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                    new ThreadFactoryBuilder().setDaemon(true).build());
            for (List<String> jobIdChunk : Lists.partition(jobNodes, jobChunkSize)) {
                executorService.submit(new MapChunkLoader(this, jobs, jobIdChunk));
            }
            logger.info("Waiting for job loading threads to finish...");
            MoreExecutors.shutdownAndAwaitTermination(executorService, 600, TimeUnit.SECONDS);
            logger.info("Job loading complete");
        }
        return jobs;
    }

    /**
     * Internal class to fetch a chunk of jobIds, then push the results into a master map
     */
    private static class MapChunkLoader implements Runnable {

        private final Map<String, IJob> jobs;
        private final List<String> chunk;
        private final JobConfigManager jobConfigManager;

        private MapChunkLoader(JobConfigManager jobConfigManager, Map<String, IJob> jobs, List<String> chunk) {
            this.jobs = jobs;
            this.chunk = chunk;
            this.jobConfigManager = jobConfigManager;
        }

        @Override
        public void run() {
            try {
                Map<String, IJob> rv = new HashMap<>();
                for (String jobId : chunk) {
                    loadChunk(rv, jobId);
                }
                synchronized (jobs) {
                    jobs.putAll(rv);
                }
            } catch (Exception e) {
                logger.error("While getting all jobs, error getting: {}", chunk, e);
                throw new RuntimeException(e);
            }
        }

        private void loadChunk(Map<String, IJob> rv, String jobId) {
            try {
                IJob jobFromQueryData =
                        jobConfigManager.createJobFromQueryData(jobId, jobConfigManager.fetchJobData(jobId));
                rv.put(jobId, jobFromQueryData);
            } catch (Exception ex) {
                logger.error("Failed while reconstituting job {}", jobId, ex);
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
            expandedConfigCache.invalidate(jobUUID);
        } catch (Exception e) {
            logger.warn("Failing to delete job, bailing", e);
            throw new RuntimeException(e);
        }
    }

    private static String getJobPath(String jobId) {
        return SPAWN_JOB_CONFIG_PATH + "/" + jobId;
    }

}

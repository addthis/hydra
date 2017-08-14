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

package com.addthis.hydra.query.spawndatastore;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpawnDataStoreHandler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SpawnDataStoreHandler.class);
    /**
     * A ZooKeeper/Priam backed data structure that keeps track of
     * the Hydra jobs in the cluster.  We are specifically
     * interested in how many tasks are in each job so we know
     * the minimum number of responses required to run a query.
     */
    private final JobConfigManager jobConfigManager;

    /**
     * A SpawnDataStore used by the JobConfigManager to fetch job status.
     * Uses either zookeeper or priam depending on the cluster config.
     */
    private final SpawnDataStore spawnDataStore;

    /**
     * Monitors ZooKeeper for query configuration information.
     * Used to determine if queries are enabled and/or traceable
     * for a given job
     */
    private final QueryConfigWatcher queryConfigWatcher;

    /**
     * A ZooKeeper backed data structure that maintains a
     * bi-directional mapping of job aliases to job IDs
     */
    private final AliasCache aliasCache;

    /**
     * a cache of job configuration data, used to reduce load placed on ZK server with high volume queries
     */
    private final LoadingCache<String, IJob> jobConfigurationCache = CacheBuilder
            .newBuilder()
            .maximumSize(2000)
            .refreshAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<String, IJob>() {
                @Override public IJob load(String key) {
                    return jobConfigManager.getJob(key);
                }
            });

    public SpawnDataStoreHandler() throws Exception {
        spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
        this.jobConfigManager = new JobConfigManager(spawnDataStore);
        this.queryConfigWatcher = new QueryConfigWatcher(spawnDataStore);
        this.aliasCache = new AliasCache();
        this.aliasCache.loadCurrentValues();
    }

    @Override public void close() {
        spawnDataStore.close();
    }

    public void validateJobForQuery(String job) {
        if (!queryConfigWatcher.safeToQuery(job)) {
            throw new QueryException("job is not safe to query (are queries enabled for this job in spawn?): " + job);
        }
    }

    public List<String> expandAlias(String job) throws ExecutionException {
        List<String> possibleJobs = aliasCache.getJobs(job);
        if ((possibleJobs != null) && !possibleJobs.isEmpty()) {
            return possibleJobs;
        }
        return Collections.singletonList(job);
    }

    public String resolveAlias(String job) throws ExecutionException {
        List<String> possibleJobs = aliasCache.getJobs(job);
        if ((possibleJobs != null) && !possibleJobs.isEmpty()) {
            return possibleJobs.get(0);
        }
        return job;
    }

    public int getCononicalTaskCount(String job) {
        IJob zkJob;
        try {
            zkJob = jobConfigurationCache.get(job);
        } catch (ExecutionException ignored) {
            throw new QueryException("unable to retrieve job configuration for job: " + job);
        }
        if (zkJob == null) {
            throw new QueryException("[MeshQueryMaster] Error:  unable to find ZK reference for job: " + job);
        }

        return new Job(zkJob).getTaskCount();
    }

}

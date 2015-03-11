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

import java.io.IOException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class SpawnDataStoreHandler {

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
    private final AliasBiMap aliasBiMap;

    /**
     * a cache of job configuration data, used to reduce load placed on ZK server with high volume queries
     */
    private final LoadingCache<String, IJob> jobConfigurationCache = CacheBuilder.newBuilder()
            .maximumSize(2000)
            .refreshAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, IJob>() {
                        public IJob load(String jobId) throws IOException {
                            return jobConfigManager.getJob(jobId);
                        }
                    }
            );

    public SpawnDataStoreHandler() throws Exception {
        spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();
        this.jobConfigManager = new JobConfigManager(spawnDataStore);
        this.queryConfigWatcher = new QueryConfigWatcher(spawnDataStore);
        this.aliasBiMap = new AliasBiMap(spawnDataStore);
        this.aliasBiMap.loadCurrentValues();
    }

    public void close() {
        spawnDataStore.close();
    }

    public Collection<IJob> getJobs() {
        return jobConfigManager.getJobs().values();
    }

    public void validateJobForQuery(String job) {
        if (!queryConfigWatcher.safeToQuery(job)) {
            throw new QueryException("job is not safe to query (are queries enabled for this job in spawn?): " + job);
        }
    }

    public List<String> expandAlias(String job) {
        List<String> possibleJobs = aliasBiMap.getJobs(job);
        if ((possibleJobs != null) && !possibleJobs.isEmpty()) {
            return possibleJobs;
        }
        return Collections.singletonList(job);
    }

    public String resolveAlias(String job) {
        List<String> possibleJobs = aliasBiMap.getJobs(job);
        if ((possibleJobs != null) && !possibleJobs.isEmpty()) {
            return possibleJobs.get(0);
        }
        return job;
    }

    /**
     * Tries to make sure enough tasks were found to satisfy the query options.
     *
     * @return The canonical task count according to spawn/ zookeeper
     */
    public int validateTaskCount(String job, Multimap<Integer, FileReference> fileReferenceMap, Set<Integer> tasks) {
        IJob zkJob;
        try {
            zkJob = jobConfigurationCache.get(job);
        } catch (ExecutionException ignored) {
            throw new QueryException("unable to retrieve job configuration for job: " + job);
        }
        if (zkJob == null) {
            final String errorMessage = "[MeshQueryMaster] Error:  unable to find ZK reference for job: " + job;
            throw new QueryException(errorMessage);
        }

        int canonicalTaskCount = new Job(zkJob).getTaskCount();
        Set<Integer> availableTasks = fileReferenceMap.keySet();
        if (availableTasks.size() != canonicalTaskCount) {
            Set<Integer> requestedTasks = getRequestedTasks(tasks, canonicalTaskCount);
            Set<Integer> missingTasks = Sets.difference(requestedTasks, availableTasks);
            if (!missingTasks.isEmpty()) {
                String err = "Did not find data for all " + requestedTasks.size() +
                             " requested tasks (and allowPartial is off): " + availableTasks.size() +
                             " available out of " + canonicalTaskCount + " total. Missing the following " +
                             missingTasks.size() + " tasks: " + missingTasks;
                throw new QueryException(err);
            }
        }
        return canonicalTaskCount;
    }

    private TreeSet<Integer> getRequestedTasks(Set<Integer> tasks, int canonicalTaskCount) {
        if (tasks.isEmpty()) {
            TreeSet<Integer> requestedTasks = new TreeSet<>();
            for (int i = 0; i < canonicalTaskCount; i++) {
                requestedTasks.add(i);
            }
            return requestedTasks;
        } else {
            return new TreeSet<>(tasks);
        }
    }

}

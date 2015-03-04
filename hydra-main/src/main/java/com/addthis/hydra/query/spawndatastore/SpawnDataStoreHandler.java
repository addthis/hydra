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
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
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
    public int validateTaskCount(String job, Multimap<Integer, FileReference> fileReferenceMap) {
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

        final int taskCount = new Job(zkJob).getTaskCount();
        int fileReferenceCount = fileReferenceMap.keySet().size();
        if (fileReferenceCount != taskCount) {
            final String errorMessage = "Did not find data for all tasks (and allowPartial is off): "
                                        + fileReferenceCount + " out of " + taskCount;
            final int numMissing = taskCount - fileReferenceCount;
            final String label = ". Missing the following " + numMissing + " tasks : ";
            final StringBuilder sb = new StringBuilder();
            final SortedMap<Integer, Collection<FileReference>> sortedMap = new TreeMap<>(fileReferenceMap.asMap());
            final Iterator<Integer> it = sortedMap.keySet().iterator();
            Integer key = it.next();
            for (int i = 0; i < taskCount; i++) {
                if ((key == null) || (i != key)) {
                    if (sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(i);
                } else {
                    key = it.hasNext() ? it.next() : null;
                }
            }
            throw new QueryException(errorMessage + label + sb.toString());
        }
        return taskCount;
    }

}

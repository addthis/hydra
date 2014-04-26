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
package com.addthis.hydra.query;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ExecutionException;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.store.AvailableCache;
import com.addthis.hydra.job.store.SpawnDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_JOB_CONFIG_PATH;


@ThreadSafe
/**
 * A class to watch job config data within the SpawnDataStore, cache it, and serve information relevant to query functions
 */
public class QueryConfigWatcher {

    /* How long job config data can live in the cache before being refreshed */
    private static final long queryConfigRefreshMillis = Parameter.longValue("query.config.refresh.millis", 15000);
    /* How many jobIds should be stored in the cache */
    private static final int queryConfigCacheSize = Parameter.intValue("query.config.cache.size", 100);

    private static final Logger log = LoggerFactory.getLogger(QueryConfigWatcher.class);

    /* A SpawnDataStore used to fetch the config data. Should be the same type (zookeeper/priam) as the one used by Spawn to store job data */
    private SpawnDataStore spawnDataStore;

    /* A LoadingCache used to save configs fetched from the SpawnDataStore */
    private final AvailableCache<JobQueryConfig> configCache;

    public QueryConfigWatcher(SpawnDataStore spawnDataStore) {
        this.spawnDataStore = spawnDataStore;
        // This cache will not block on queryconfig fetches unless no data has been fetched for that job before
        this.configCache = new AvailableCache<JobQueryConfig>(queryConfigRefreshMillis, -1, queryConfigCacheSize, 2) {
            @Override
            public JobQueryConfig fetchValue(String id) {
                return fetchJobQueryConfig(id);
            }
        };
    }

    /**
     * Fetch the query config for a jobId, using the cached result if possible
     *
     * @param jobId The jobId to fetch
     * @return A (possibly null) JobQueryConfig
     */
    public JobQueryConfig getJobQueryConfig(String jobId) {
        try {
            return configCache.get(jobId);
        } catch (ExecutionException e) {
            if (log.isDebugEnabled()) {
                log.debug("Exception during JobQueryConfig fetch: " + e, e);
            }
            // JobQueryConfig was not found, return null
            return null;
        }
    }

    /**
     * Throw away the contents of the cache, primarily for testing
     */
    public void invalidateQueryConfigCache() {
        configCache.clear();
    }

    /**
     * Query the SpawnDataStore for the config for a job. This method should not block getJobQueryConfig unless the
     * config has never been queried before
     *
     * @param jobId The jobId to fetch
     * @return The JobQueryConfig if one was found, or null otherwise
     */
    private JobQueryConfig fetchJobQueryConfig(String jobId) {
        JobQueryConfig jobQueryConfig = new JobQueryConfig();
        String jobConfigPath = SPAWN_JOB_CONFIG_PATH + "/" + jobId + "/queryconfig";
        return spawnDataStore.loadCodable(jobConfigPath, jobQueryConfig) ? jobQueryConfig : null;
    }

    /**
     * Check whether a job is safe to query, using the cache if possible
     *
     * @param jobID The jobId to check
     * @return True if the job is queryable
     */
    public boolean safeToQuery(String jobID) {
        JobQueryConfig jqc = getJobQueryConfig(jobID);
        return jqc != null && jqc.getCanQuery();
    }

    /**
     * Check whether job data exists for a jobId, using the cache if possible
     *
     * @param jobID The jobId to check
     * @return True if any query config data exists
     */
    public boolean jobIsTracked(String jobID) {
        JobQueryConfig jqc = getJobQueryConfig(jobID);
        return jqc != null;
    }

    /**
     * Check whether a jobId should be traced according to its query config data
     *
     * @param jobID The jobId to check
     * @return True if it should be traced
     */
    public boolean shouldTrace(String jobID) {
        JobQueryConfig jqc = getJobQueryConfig(jobID);
        return jqc != null && jqc.getQueryTraceLevel() > 0;
    }

}

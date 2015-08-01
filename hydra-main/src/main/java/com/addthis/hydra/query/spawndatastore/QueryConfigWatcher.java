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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ExecutionException;

import com.addthis.basis.util.Parameter;

import com.addthis.bark.StringSerializer;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.store.AvailableCache;
import com.addthis.hydra.job.store.SpawnDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.codec.json.CodecJSON.INSTANCE;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_JOB_CONFIG_PATH;


/**
 * A class to watch job config data within the SpawnDataStore, cache it, and serve information relevant to
 * query functions
 */
@ThreadSafe
public class QueryConfigWatcher {

    private static final Logger log = LoggerFactory.getLogger(QueryConfigWatcher.class);

    /* How long job config data can live in the cache before being refreshed */
    private static final long QUERY_CONFIG_REFRESH_MILLIS = Parameter.longValue("query.config.refresh.millis", 15000);
    /* How many jobIds should be stored in the cache */
    private static final int QUERY_CONFIG_CACHE_SIZE = Parameter.intValue("query.config.cache.size", 100);

    /* A SpawnDataStore used to fetch the config data. Should be the same type (zookeeper/priam)
    as the one used by Spawn to store job data */
    private final SpawnDataStore spawnDataStore;

    /* A LoadingCache used to save configs fetched from the SpawnDataStore */
    private final AvailableCache<JobQueryConfig> configCache;

    public QueryConfigWatcher(SpawnDataStore spawnDataStore) {
        this.spawnDataStore = spawnDataStore;
        // This cache will not block on queryconfig fetches unless no data has been fetched for that job before
        this.configCache =
                new AvailableCache<JobQueryConfig>(QUERY_CONFIG_REFRESH_MILLIS, -1, QUERY_CONFIG_CACHE_SIZE, 2) {
                    @Nullable @Override public JobQueryConfig fetchValue(String id) {
                        JobQueryConfig jobQueryConfig = new JobQueryConfig();
                        String jobConfigPath = SPAWN_JOB_CONFIG_PATH + "/" + id + "/queryconfig";
                        String raw = QueryConfigWatcher.this.spawnDataStore.get(jobConfigPath);
                        if (raw == null) {
                            return null;
                        }
                        try {
                            INSTANCE.decode(jobQueryConfig, StringSerializer.deserialize(raw.getBytes()).getBytes());
                            return jobQueryConfig;
                        } catch (Exception e) {
                            log.warn("Failed to decode query config", e);
                            return null;
                        }
                    }
                };
    }

    /**
     * Fetch the query config for a jobId, using the cached result if possible
     *
     * @param jobId The jobId to fetch
     * @return A (possibly null) JobQueryConfig
     */
    @Nullable public JobQueryConfig getJobQueryConfig(String jobId) {
        try {
            return configCache.get(jobId);
        } catch (ExecutionException e) {
            log.warn("Exception during JobQueryConfig fetch", e);
            // JobQueryConfig was not found, return null
            return null;
        }
    }

    /** Throw away the contents of the cache, primarily for testing */
    public void invalidateQueryConfigCache() {
        configCache.clear();
    }

    /**
     * Check whether a job is safe to query, using the cache if possible
     *
     * @param jobID The jobId to check
     * @return True if the job is queryable
     */
    public boolean safeToQuery(String jobID) {
        JobQueryConfig jqc = getJobQueryConfig(jobID);
        return (jqc != null) && jqc.getCanQuery();
    }
}

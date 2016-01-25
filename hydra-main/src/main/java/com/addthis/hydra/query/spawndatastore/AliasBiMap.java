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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.store.AvailableCache;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
/**
 * A class for maintaining a two-way map between aliases and jobIds, used by both Spawn and Mqmaster to stay in sync
 */
public class AliasBiMap {

    private static final Logger log = LoggerFactory.getLogger(AliasBiMap.class);

    public static final String ALIAS_PATH = "/query/alias";

    /* This SpawnDataStore must be the same type (zookeeper/priam) between Spawn and Mqmaster. This should
     * be guaranteed by the implementation of DataStoreUtil. */
    private final SpawnDataStore spawnDataStore;
    private final HashMap<String, List<String>> alias2jobs;
    private final HashMap<String, String> job2alias;
    private final ObjectMapper mapper;
    private final ReentrantLock mapLock;
    private final AvailableCache<String> mapCache;

    public AliasBiMap() throws Exception {
        this(DataStoreUtil.makeCanonicalSpawnDataStore());
        loadCurrentValues();
    }

    public AliasBiMap(SpawnDataStore spawnDataStore) {
        this.spawnDataStore = spawnDataStore;
        mapLock = new ReentrantLock();
        mapper = new ObjectMapper();
        alias2jobs = new HashMap<>();
        job2alias = new HashMap<>();
        /* The interval to refresh cached alias values */
        long cacheRefresh = Parameter.longValue("alias.bimap.refresh", 10000);
        /* The expiration period for cache values. Off by default, but useful for testing. */
        long cacheExpire = Parameter.longValue("alias.bimap.expire", -1);
        /* The max size of the alias cache */
        int cacheSize = Parameter.intValue("alias.bimap.cache.size", 1000);
        mapCache = new AvailableCache<String>(cacheRefresh, cacheExpire, cacheSize, 2) {
            @Override public String fetchValue(String id) {
                try {
                    return updateAlias(id, AliasBiMap.this.spawnDataStore.getChild(ALIAS_PATH, id));
                } catch (Exception e) {
                    log.warn("Exception when fetching alias: {}", id, e);
                    return updateAlias(id, null);
                }
            }
        };
    }

    @VisibleForTesting
    protected List<String> decodeAliases(@Nonnull Object data) {
        try {
            return mapper.readValue(data.toString(), new TypeReference<List<String>>() {});
        } catch (IOException e) {
            log.warn("Failed to decode data", e);
            return new ArrayList<>(0);
        }
    }

    /**
     * Read from the SpawnDataStore to determine the newest values of the alias map
     */
    public void loadCurrentValues() {
        mapLock.lock();
        try {
            alias2jobs.clear();
            job2alias.clear();
            Map<String, String> aliases = spawnDataStore.getAllChildren(ALIAS_PATH);
            if ((aliases == null) || aliases.isEmpty()) {
                log.warn("No aliases found, unless this is on first cluster startup something is probably wrong");
                return;
            }
            mapCache.clear();
            for (Map.Entry<String, String> aliasEntry : aliases.entrySet()) {
                mapCache.put(aliasEntry.getKey(), aliasEntry.getValue());
                updateAlias(aliasEntry.getKey(), aliasEntry.getValue());
            }
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * Refresh an alias based on the latest cached value
     *
     * @param alias The alias to refresh
     */
    private void refreshAlias(String alias) {
        try {
            updateAlias(alias, mapCache.get(alias));
        } catch (ExecutionException e) {
            log.warn("Failed to refresh alias: {}", alias, e);
        }
    }

    /**
     * Load the jobIds for a particular alias from the SpawnDataStore
     *
     * @param alias The alias key to check
     * @return String The data that was updated (so the cache can be updated)
     */
    @Nullable private String updateAlias(String alias, @Nullable String data) {
        if (alias == null) {
            return data;
        }
        if ((data == null) || data.isEmpty()) {
            deleteAlias(alias);
            return data;
        }
        List<String> jobs = decodeAliases(data);
        if (jobs.isEmpty()) {
            log.warn("no jobs for alias {}, ignoring {}", alias, alias);
            return data;
        }
        mapLock.lock();
        try {
            alias2jobs.put(alias, jobs);
            job2alias.put(jobs.get(0), alias);
        } finally {
            mapLock.unlock();
        }
        return data;
    }

    /**
     * Get an unmodifiable view of the current Alias map
     *
     * @return A map describing alias name => jobIds
     */
    public Map<String, List<String>> viewAliasMap() {
        mapLock.lock();
        try {
            return Collections.unmodifiableMap(alias2jobs);
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * Update the SpawnDataStore with a new alias value
     *
     * @param alias The alias to add/change
     * @param jobs  The jobs to store under that alias
     */
    public void putAlias(String alias, List<String> jobs) {
        mapLock.lock();
        try {
            alias2jobs.put(alias, jobs);
            job2alias.put(jobs.get(0), alias);
            StringWriter sw = new StringWriter();
            mapper.writeValue(sw, jobs);
            spawnDataStore.putAsChild(ALIAS_PATH, alias, sw.toString());
        } catch (Exception e) {
            log.warn("failed to put alias: {}", alias, e);
            throw Throwables.propagate(e);
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * Delete the data for a given alias
     *
     * @param alias The alias to check
     */
    public void deleteAlias(String alias) {
        mapLock.lock();
        try {
            List<String> jobs = alias2jobs.get(alias);
            alias2jobs.remove(alias);
            if ((jobs != null) && !jobs.isEmpty()) {
                for (String job : jobs) {
                    String aliasVal = job2alias.get(job);
                    if (Objects.equals(aliasVal, alias)) {
                        job2alias.remove(job);
                    }
                }
            }
        } finally {
            mapLock.unlock();
        }
        spawnDataStore.deleteChild(ALIAS_PATH, alias);
        mapCache.remove(alias);
    }

    /**
     * Test a job/alias pair to see if an alias has disappeared
     *
     * @param job   The job to test
     * @param alias The alias to check
     */
    private void checkAlias(String job, String alias) {
        mapLock.lock();
        try {
            if (!alias2jobs.containsKey(alias) && job2alias.get(job).equals(alias)) {
                job2alias.remove(job);
            }
        } finally {
            mapLock.unlock();
        }

    }

    /**
     * Get all jobIds for a given alias
     *
     * @param alias The alias to check
     * @return A list of jobIds, possible null
     */
    public List<String> getJobs(String alias) {
        refreshAlias(alias);
        mapLock.lock();
        try {
            return alias2jobs.get(alias);
        } finally {
            mapLock.unlock();
        }

    }

    /**
     * Get an alias for a particular jobId
     *
     * @param jobid The jobId to check
     * @return One of the aliases for that job
     */
    public String getLikelyAlias(String jobid) {
        mapLock.lock();
        try {
            String tmpAlias = job2alias.get(jobid);
            if (tmpAlias != null) {
                // Check to see if the alias has been deleted
                checkAlias(jobid, tmpAlias);
            }
            return job2alias.get(jobid);
        } finally {
            mapLock.unlock();
        }

    }

}

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
package com.addthis.hydra.job.alias;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.query.spawndatastore.AliasCache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class AliasManagerImpl implements AliasManager {

    private static final Logger log = LoggerFactory.getLogger(AliasManagerImpl.class);

    //private AliasBiMap aliasBiMap;
    private Map<String, String> aliases;

    public static final String ALIAS_PATH = "/query/alias";
    /* This SpawnDataStore must be the same type (zookeeper/priam) between Spawn and Mqmaster. This should
         * be guaranteed by the implementation of DataStoreUtil. */
    private final SpawnDataStore spawnDataStore;
    private final HashMap<String, List<String>> alias2jobs;
    private final HashMap<String, String> job2alias;
    public final ObjectMapper mapper;
    private final ReentrantLock mapLock;

    public AliasManagerImpl() throws Exception{
        this.spawnDataStore = DataStoreUtil.makeCanonicalSpawnDataStore();   // added
        this.mapLock = new ReentrantLock();
        this.mapper = new ObjectMapper();
        this.alias2jobs = new HashMap<>();
        this.job2alias = new HashMap<>();
    }

    public AliasManagerImpl(SpawnDataStore spawnDataStore) {
//        aliasBiMap.loadCurrentValues();
        this.spawnDataStore = spawnDataStore;   // added
        this.mapLock = new ReentrantLock();
        this.mapper = new ObjectMapper();
        this.alias2jobs = new HashMap<>();
        this.job2alias = new HashMap<>();
    }

    /**
     * Returns a map describing alias name => jobIds
     */
    public Map<String, List<String>> getAliases() {
        aliases = spawnDataStore.getAllChildren(ALIAS_PATH);

        Map<String, List<String>> alias2Jobs = new HashMap<>();

        for (Map.Entry<String, String> aliasEntry : aliases.entrySet()) {
            String key = aliasEntry.getKey();
            List<String> jobs = decodeAliases(aliasEntry.getValue());
            alias2Jobs.put(key, jobs);
        }
        return alias2Jobs;
    }

    // getJobs
    public List<String> aliasToJobs(String alias) {
        return getJobs(alias);
    }

    /**
     * Updates the full job id list of an alias.
     * 
     * This method does nothing if the give job id list is empty.
     */
    public void addAlias(String alias, List<String> jobs) {
        if (jobs.size() > 0) {
            putAlias(alias, jobs);
        } else {
            log.warn("Ignoring empty jobs addition for alias: {}", alias);
        }
    }

    // --------------------------------------------------
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
     * Refresh an alias based on the latest cached value
     *
     * @param alias The alias to refresh
     */
    private void refreshAlias(String alias) {
        try {
            String data = this.spawnDataStore.getChild(ALIAS_PATH, alias);
            updateAlias(alias, data);
        } catch (ExecutionException e) {
            log.warn("Failed to refresh alias: {}", alias, e);
        } catch (Exception e) {
            e.printStackTrace();
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

    @VisibleForTesting public List<String> decodeAliases(@Nonnull Object data) {
        try {
            return mapper.readValue(data.toString(), new TypeReference<List<String>>() {});
        } catch (IOException e) {
            log.warn("Failed to decode data", e);
            return new ArrayList<>(0);
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

        try {
            String sJobsAll = spawnDataStore.getChild(ALIAS_PATH, alias);
            System.out.println("sJobs from datastore for " + alias + " = " +  sJobsAll);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // todo: delete cache since refresh is initiated when query
        AliasCache ac = null;
        try {
            ac = new AliasCache();
            ac.deleteAlias(alias);

            List<String> jobs = ac.getJobs(alias);
            if(jobs == null ) {
                System.out.println("jobs is null");
                return;
            }

            if(jobs.size() == 0) {
                System.out.println("jobs size is 0");
                return;
            }

            System.out.println("jobs after remove alias " + alias + " = " + jobs);

        } catch (Exception e) {
            e.printStackTrace();
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
}

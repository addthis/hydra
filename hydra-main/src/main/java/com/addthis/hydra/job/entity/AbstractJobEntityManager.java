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
package com.addthis.hydra.job.entity;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import com.addthis.basis.collect.ConcurrentHashMapV8;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.store.SpawnDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link JobEntityManager} implementations.
 * 
 * @param <T> type of entity
 */
public abstract class AbstractJobEntityManager<T> implements JobEntityManager<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final ConcurrentMap<String, T> entities = new ConcurrentHashMapV8<>();

    private final Spawn spawn;
    private final SpawnDataStore spawnDataStore;
    private final Class<T> entityClass;
    private final String dataStorePath;
    private final String entityClassName;

    /**
     * Constructs a new instance and load entities from {@link SpawnDataStore}.
     * 
     * @param entityClass   the entity class
     * @param dataStorePath the path under which to load/store entities from/to in the data store 
     * @throws Exception if any error occurred when loading entities from persistent data store. 
     */
    public AbstractJobEntityManager(
            Spawn spawn,
            Class<T> entityClass,
            String dataStorePath) throws Exception {
        this.spawn = spawn;
        this.spawnDataStore = spawn.getSpawnDataStore();
        this.entityClass = entityClass;
        this.dataStorePath = dataStorePath;
        this.entityClassName = entityClass.getSimpleName();
        loadEntities(spawnDataStore);
    }

    private void loadEntities(SpawnDataStore spawnDataStore) throws Exception {
        log.info("Loading {}s from data store...", entityClassName);
        Map<String, String> loadedEntities = spawnDataStore.getAllChildren(dataStorePath);
        if (loadedEntities == null) {
            log.warn("No {}s loaded from data store.", entityClassName);
            return;
        }
        for (Entry<String, String> entry : loadedEntities.entrySet()) {
            String json = entry.getValue();
            if (json != null && !json.equals("null") && !json.isEmpty()) {
                T entity = CodecJSON.decodeString(entityClass, json);
                putEntity(entry.getKey(), entity, false);
            }
        }
        log.info("{} {}s loaded from data store", size(), entityClassName);
    }
    
    public String getDataStorePath() {
        return dataStorePath;
    }

    @Override
    public int size() {
        return entities.size();
    }

    @Override
    public Collection<String> getKeys() {
        return entities.keySet();
    }

    @Override
    public T getEntity(String key) {
        return entities.get(key.trim());
    }

    @Override
    public void putEntity(String key, T entity, boolean store) throws Exception {
        key = key.trim();
        if (entities.put(key, entity) == null) {
            log.info("Added new {} {}", entityClassName, key);
        } else {
            log.info("Updated existing {} {}", entityClassName, key);
        }
        if (store) {
            spawnDataStore.putAsChild(dataStorePath, key, CodecJSON.encodeString(entity));
        }
    }

    @Override
    public boolean deleteEntity(String key) {
        // prevent deletion of entity used in jobs
        Job job = findDependentJob(spawn, key);
        if (job != null) {
            log.warn("Unable to delete {} {} because it is used by job {}", entityClassName, key, job.getId());
            return false;
        }

        T entity = entities.remove(key);
        if (entity != null) {
            spawnDataStore.deleteChild(dataStorePath, key);
            log.info("Successfully deleted {} {}", entityClassName, key);
            return true;
        } else {
            log.warn("Unable to delete {} {} because it doesn't exist", entityClassName, key);
            return false;
        }
    }

    /** Returns {@link true} if the given entity key is used by any job. */
    protected abstract Job findDependentJob(Spawn spawn, String entityKey);

}

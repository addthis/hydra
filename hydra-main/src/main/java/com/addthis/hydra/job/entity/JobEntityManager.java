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

/**
 * Provides standard CRUD operations for job entities.
 * 
 * Job entities are things a job can reference by key, such as {@link JobMacro}s and 
 * {@link JobCommand}s.
 * 
 * @param <T> type of entity
 */
public interface JobEntityManager<T> {

    /** Returns the total number of entities. */
    int size();

    /** Returns all entity keys. */
    Collection<String> getKeys();

    /** Returns an entity by key. {@code null} if not found. */
    T getEntity(String key);

    /**
     * Adds or updates an entity, and optionally saves to the persistent spawn data store.
     * 
     * @param store {@code true} to save to persistent data store; {@code false} otherwise.
     * @throws Exception if error occurred saving to the data store.
     */
    void putEntity(String key, T entity, boolean store) throws Exception;

    /** 
     * Deletes an entity by key if it is not used by any job.
     * 
     * @return  {@code true} if entity is deleted successfully; {@code false} if entity is being 
     *          used by a job therefore can't be deleted.
     */
    boolean deleteEntity(String key);

}

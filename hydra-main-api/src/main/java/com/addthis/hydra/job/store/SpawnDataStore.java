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
package com.addthis.hydra.job.store;

import java.util.List;
import java.util.Map;

import com.addthis.codec.Codec;

/**
 * This interface describes methods for storing key/value data within various storage solutions
 */
public interface SpawnDataStore {

    /**
     * A description for the DataStore, mainly for logging. e.g. "zookeeper"
     *
     * @return A short String description
     */
    public abstract String getDescription();

    /**
     * Get the value stored at a certain path
     *
     * @param path The path to check
     * @return The String value of the path, or null if there is no data there
     */
    public abstract String get(String path);

    /**
     * Get the values stored at multiple paths
     *
     * @param paths An array of ids that should be fetched
     * @return A map describing {id : value} for any ids that were found
     */
    public abstract Map<String, String> get(String[] paths);

    /**
     * Put a value into a certain path
     *
     * @param path  The path to write to
     * @param value The value to write
     * @throws Exception If there is a problem writing to the path
     */
    public abstract void put(String path, String value) throws Exception;

    /**
     * Put a value as a child of a certain path, that can later be identified along with any other children that might exist
     *
     * @param parent  The parent location
     * @param childId The identifier for this particular child
     * @param value   The value that should be stored within the child path
     * @throws Exception If there is a problem writing to the path
     */
    public abstract void putAsChild(String parent, String childId, String value) throws Exception;

    /**
     * Load the value of a path into a Codable object
     *
     * @param path  The location to read from
     * @param shell A Shell codable to deserialize onto
     * @param <T>   The particular type of Codable
     * @return True if the value was successfully loaded
     */
    public abstract <T extends Codec.Codable> boolean loadCodable(String path, T shell);

    /**
     * Get the value of a certain child of a certain path
     *
     * @param parent  The location of the parent
     * @param childId The identifier for this particular child
     * @return A String if the child existed, or null otherwise
     * @throws Exception If there is a problem reading the child
     */
    public abstract String getChild(String parent, String childId) throws Exception;

    /**
     * Delete a child of a certain path
     *
     * @param parent  The path to the parent
     * @param childId The identifier for this particular child
     */
    public abstract void deleteChild(String parent, String childId);

    /**
     * Delete a path and any data stored there
     *
     * @param path The path to be deleted
     */
    public abstract void delete(String path);

    /**
     * Get the ids of all children that have been stored for a particular path
     *
     * @param path The path to check
     * @return A list of child ids, or null if none exist
     */
    public abstract List<String> getChildrenNames(String path);

    /**
     * Get all children, ids and values, for a particular path
     *
     * @param path The path to fetch
     * @return A map describing all children and their values
     */
    public abstract Map<String, String> getAllChildren(String path);

    /**
     * Perform any cleanup operations necessary for this data store.
     */
    public abstract void close();
}

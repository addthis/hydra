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
package com.addthis.hydra.job.spawn;

import java.util.Optional;
import java.util.Properties;

import com.addthis.hydra.job.store.DataStoreUtil.DataStoreType;

/** Provides access to various system functions, settings and states. */
public interface SystemManager {

    /** debug output, can be disabled by policy */
    public boolean debug(String match);
    
    public void updateDebug(Optional<String> debug);

    public void updateQueryHost(Optional<String> queryHost);

    public void updateSpawnHost(Optional<String> spawnHost);

    public void updateDisabled(Optional<String> disabled);

    public void updateSslEnabled(boolean enabled);

    public Settings getSettings();

    public String getSpawnHost();

    /** Returns git properties */
    public Properties getGitProperties();

    /** Returns the priority level required for jobs to transition from queued to running. */
    public int quiescentLevel();

    /** Returns {@code true} if the quiescentLevel is nonzero. */
    public boolean isQuiesced();

    /** Set the quiesce status of the cluster and returns the new status */
    public boolean quiesceCluster(boolean quiesce, String username);

    /**
     * Performs spawn health check and returns the result.
     * 
     * Applies the specified retry for applicable checks that may return false negative.
     */
    public HealthCheckResult healthCheck(int retries) throws Exception;

    /** Performs data store cutover */
    public void cutoverDataStore(
            DataStoreType sourceType,
            DataStoreType targetType,
            boolean checkAllWrites) throws Exception;

}

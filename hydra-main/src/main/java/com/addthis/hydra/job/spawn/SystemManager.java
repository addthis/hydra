/*
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 * 
 * You can obtain a copy of the license at
 * http://www.opensource.org/licenses/cddl1.php
 * See the License for the specific language governing
 * permissions and limitations under the License.
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

    /** Returns git properties */
    public Properties getGitProperties();
    
    /** Returns {@code true} if spawn is quiesced */
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

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

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_BALANCE_PARAM_PATH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.InputStream;

import java.util.Properties;

import com.addthis.basis.util.Strings;

import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.DataStoreUtil.DataStoreType;
import com.addthis.hydra.job.store.SpawnDataStoreKeys;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemManagerImpl implements SystemManager {

    private static final Logger log = LoggerFactory.getLogger(SystemManagerImpl.class);
    private static final String GIT_PROPS = "/hydra-git.properties";

    private final Spawn spawn;
    private volatile Properties gitProperties;

    public SystemManagerImpl(Spawn spawn) {
        this.spawn = spawn;
    }

    @Override
    public Properties getGitProperties() {
        if (gitProperties == null) {
            gitProperties = remapRawGitProperties(loadRawGitProperties(GIT_PROPS));
        }
        return gitProperties;
    }

    private Properties loadRawGitProperties(String loc) {
        Properties p = new Properties();
        try (InputStream in = getClass().getResourceAsStream(loc)) {
            p.load(in);
        } catch (Exception e) {
            log.warn("Error loading {}, possibly jar was not compiled with maven.", GIT_PROPS, e);
        }
        return p;
    }

    private Properties remapRawGitProperties(Properties raw) {
        Properties p = new Properties();
        p.put("commitIdAbbrev", Strings.unNull(raw.getProperty("git.commit.id.abbrev")));
        p.put("commitUserEmail", Strings.unNull(raw.getProperty("git.commit.user.email")));
        p.put("commitMessageFull", Strings.unNull(raw.getProperty("git.commit.message.full")));
        p.put("commitId", Strings.unNull(raw.getProperty("git.commit.id")));
        p.put("commitUserName", Strings.unNull(raw.getProperty("git.commit.user.name")));
        p.put("buildUserName", Strings.unNull(raw.getProperty("git.build.user.name")));
        p.put("commitIdDescribe", Strings.unNull(raw.getProperty("git.commit.id.describe")));
        p.put("buildUserEmail", Strings.unNull(raw.getProperty("git.build.user.email")));
        p.put("branch", Strings.unNull(raw.getProperty("git.branch")));
        p.put("commitTime", Strings.unNull(raw.getProperty("git.commit.time")));
        p.put("buildTime", Strings.unNull(raw.getProperty("git.build.time")));
        return p;
    }

    @Override
    public boolean quiesceCluster(boolean quiesce, String username) {
        setQueisced(quiesce);
        spawn.sendClusterQuiesceEvent(username);
        return spawn.spawnState.quiesce.get();
    }
    
    private void setQueisced(boolean quiesced) {
        SpawnMetrics.quiesceCount.clear();
        if (quiesced) {
            SpawnMetrics.quiesceCount.inc();
        }
        spawn.spawnState.quiesce.set(quiesced);
        spawn.writeState();
    }

    @Override
    public SpawnBalancerConfig getSpawnBalancerConfig() {
        return spawn.getSpawnBalancer().getConfig();
    }

    @Override
    public void setSpawnBalancerConfig(SpawnBalancerConfig config) {
        spawn.getSpawnBalancer().setConfig(config);
        spawn.getSpawnBalancer().saveConfigToDataStore();
    }

    @Override
    public void setHostFailWorkerObeyTaskSlots(boolean obey) {
        spawn.getHostFailWorker().setObeyTaskSlots(obey);

    }

    @Override
    public HealthCheckResult healthCheck(int retries) throws Exception {
        return new HealthCheckResult().setDataStoreOK(checkDataStore(retries));
    }

    /**
     * Validates data store has the most recent update.
     * 
     * This method supports retry, as data store may not have been updated due to unfortunate 
     * timing.
     */
    private boolean checkDataStore(int retries) throws Exception {
        for (int i = 0; i <= Math.max(retries, 0); i++) {
            if (validateDateStore(spawn)) {
                return true;
            } else {
                Thread.sleep(100);
            }
        }
        return false;
    }

    /** 
     * Validates data store update by comparing the most recent job submitTime in memory to that of 
     * persisted in the data store.
     */
    private boolean validateDateStore(Spawn spawn) throws Exception {
        Job job = getMostRecentlySubmittedJob(spawn);
        if (job != null) {
            String s = spawn.getSpawnDataStore().getChild(SpawnDataStoreKeys.SPAWN_JOB_CONFIG_PATH,
                    job.getId());
            JsonNode json = Jackson.defaultCodec().getObjectMapper().readTree(s);
            boolean result = job.getSubmitTime().equals(json.get("submitTime").asLong());
            log.info("Data store integrity based on submitTime of job {}: {}", job.getId(), result);
            return result;
        } else {
            log.warn("Unable to find a suitable job to use for validating data store integrity");
            return false;
        }
    }

    private Job getMostRecentlySubmittedJob(Spawn spawn) {
        Job mostRecentJob = null;
        for (Job job : spawn.listJobsConcurrentImmutable()) {
            if (job.getSubmitTime() != null) {
                if (mostRecentJob == null || job.getSubmitTime() > mostRecentJob.getSubmitTime()) {
                    mostRecentJob = job;
                }
            }
        }
        return mostRecentJob;
    }

    @Override
    public void cutoverDataStore(
            DataStoreType sourceType,
            DataStoreType targetType,
            boolean checkAllWrites) throws Exception {
        checkState(spawn.getQuiesced(), "Spawn must be quiesced to cut over stored data");
        checkArgument(sourceType != null, "Source data store type must be specified");
        checkArgument(targetType != null, "Target data store type must be specified");
        DataStoreUtil.cutoverBetweenDataStore(DataStoreUtil.makeSpawnDataStore(sourceType),
                DataStoreUtil.makeSpawnDataStore(targetType), checkAllWrites);
    }

}

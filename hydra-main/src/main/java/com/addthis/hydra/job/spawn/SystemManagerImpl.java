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

import java.io.InputStream;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.DataStoreUtil.DataStoreType;
import com.addthis.hydra.job.store.SpawnDataStoreKeys;
import com.addthis.hydra.job.web.SpawnServiceConfiguration;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SystemManagerImpl implements SystemManager {

    private static final Logger log = LoggerFactory.getLogger(SystemManagerImpl.class);
    private static final String GIT_PROPS = "/hydra-git.properties";

    private final Spawn spawn;
    private final int authenticationTokenTimeout;
    private final int authenticationSudoTimeout;

    private String debug;
    private String queryHost;
    private String spawnHost;
    private String meshHttpHost;
    private boolean sslEnabled;

    private volatile Properties gitProperties;

    public SystemManagerImpl(Spawn spawn, String debug, String queryHost, String spawnHost, String meshHttpHost,
                             int authenticationTokenTimeout, int authenticationSudoTimeout) {
        this.spawn = spawn;
        this.debug = debug;
        this.queryHost = queryHost;
        this.spawnHost = spawnHost;
        this.meshHttpHost = meshHttpHost;
        this.authenticationTokenTimeout = authenticationTokenTimeout;
        this.authenticationSudoTimeout = authenticationSudoTimeout;
    }

    @Override
    public boolean debug(String match) {
        return debug != null && (debug.contains(match) || debug.contains("-all-"));
    }

    @Override
    public void updateSslEnabled(boolean enabled) { sslEnabled = enabled; }

    @Override
    public void updateDebug(Optional<String> opt) {
        opt.ifPresent(v -> debug = v);
    }

    @Override
    public void updateQueryHost(Optional<String> opt) {
        opt.ifPresent(v -> queryHost = v);
    }

    @Override
    public void updateSpawnHost(Optional<String> opt) {
        opt.ifPresent(v -> spawnHost = v);
    }

    @Override
    public void updateDisabled(Optional<String> opt) {
        opt.ifPresent(v -> {
            List<String> newDisabledHosts = Splitter.on(',').omitEmptyStrings().splitToList(v);
            spawn.spawnState.disabledHosts.addAll(newDisabledHosts);
            spawn.spawnState.disabledHosts.retainAll(newDisabledHosts);
            spawn.writeState();
        });
    }

    @Override
    public Properties getGitProperties() {
        if (gitProperties == null) {
            gitProperties = remapRawGitProperties(loadRawGitProperties(GIT_PROPS));
        }
        return gitProperties;
    }

    @Override
    public String getSpawnHost() {
        return spawnHost;
    }

    @Override
    public Settings getSettings() {
        String disabled = Joiner.on(',').skipNulls().join(spawn.spawnState.disabledHosts);
        return new Settings.Builder().setDebug(debug)
                                     .setQuiesce(isQuiesced())
                                     .setQueryHost(queryHost)
                                     .setSpawnHost(spawnHost)
                                     .setMeshHttpHost(meshHttpHost)
                                     .setDisabled(disabled)
                                     .setDefaultReplicaCount(Spawn.DEFAULT_REPLICA_COUNT)
                                     .setSslDefault(sslEnabled && SpawnServiceConfiguration.SINGLETON.defaultSSL)
                                     .setAuthTimeout(authenticationTokenTimeout)
                                     .setSudoTimeout(authenticationSudoTimeout)
                                     .build();
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
        p.put("commitIdAbbrev", Strings.nullToEmpty(raw.getProperty("git.commit.id.abbrev")));
        p.put("commitUserEmail", Strings.nullToEmpty(raw.getProperty("git.commit.user.email")));
        p.put("commitMessageFull", Strings.nullToEmpty(raw.getProperty("git.commit.message.full")));
        p.put("commitId", Strings.nullToEmpty(raw.getProperty("git.commit.id")));
        p.put("commitUserName", Strings.nullToEmpty(raw.getProperty("git.commit.user.name")));
        p.put("buildUserName", Strings.nullToEmpty(raw.getProperty("git.build.user.name")));
        p.put("commitIdDescribe", Strings.nullToEmpty(raw.getProperty("git.commit.id.describe")));
        p.put("buildUserEmail", Strings.nullToEmpty(raw.getProperty("git.build.user.email")));
        p.put("branch", Strings.nullToEmpty(raw.getProperty("git.branch")));
        p.put("commitTime", Strings.nullToEmpty(raw.getProperty("git.commit.time")));
        p.put("buildTime", Strings.nullToEmpty(raw.getProperty("git.build.time")));
        return p;
    }

    @Override public int quiescentLevel() {
        return spawn.spawnState.quiescentLevel.get();
    }

    @Override
    public boolean isQuiesced() {
        return spawn.spawnState.quiescentLevel.get() > 0;
    }

    @Override
    public boolean quiesceCluster(boolean quiesce, String username) {
        int desiredQuiescentLevel;
        if (quiesce) {
            desiredQuiescentLevel = 100;
        } else {
            desiredQuiescentLevel = 0;
        }
        if (spawn.spawnState.quiescentLevel.compareAndSet(100 - desiredQuiescentLevel, desiredQuiescentLevel)) {
            SpawnMetrics.quiesceCount.clear();
            if (quiesce) {
                SpawnMetrics.quiesceCount.inc();
            }
            spawn.writeState();
            spawn.sendClusterQuiesceEvent(username);
            log.info("User {} has {} the cluster.", username, (quiesce ? "quiesced" : "unquiesed"));
        }
        return isQuiesced();
    }

    @Override
    public HealthCheckResult healthCheck(int retries) throws Exception {
        return new HealthCheckResult().setDataStoreOK(checkDataStore(retries))
                                      .setAlertCheckOK(spawn.getJobAlertManager().isAlertEnabledAndWorking());
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
        checkState(isQuiesced(), "Spawn must be quiesced to cut over stored data");
        checkArgument(sourceType != null, "Source data store type must be specified");
        checkArgument(targetType != null, "Target data store type must be specified");
        DataStoreUtil.cutoverBetweenDataStore(DataStoreUtil.makeSpawnDataStore(sourceType),
                DataStoreUtil.makeSpawnDataStore(targetType), checkAllWrites);
    }
}

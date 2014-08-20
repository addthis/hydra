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

import java.io.File;
import java.io.IOException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.maljson.JSONArray;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing job config versioning.
 */
public class JobStore {

    private final File jobStoreDir;
    private static final Logger log = LoggerFactory.getLogger(JobStore.class);
    private final JobStoreGit jobStoreGit;
    private static final String noDiffMessage = "No changes detected.";
    private final Cache<String, String> cachedConfigHash;
    private static final int maxCacheSize = Parameter.intValue("job.store.cache.size", 50);

    /**
     * Instantiate the jobStore object, including the git repo and related directories
     *
     * @param jobStoreDir The directory that will store job config files
     * @throws Exception If there is a problem instantiating the git store
     */
    public JobStore(File jobStoreDir) throws Exception {
        this.jobStoreDir = jobStoreDir;
        if (!jobStoreDir.exists()) {
            Files.initDirectory(jobStoreDir);
        }
        jobStoreGit = new JobStoreGit(jobStoreDir);
        cachedConfigHash = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build();
    }

    /**
     * Get a history of changes for the given job
     *
     * @param jobId The job ID to look for
     * @return A JSON array of the form [{commit:commitid, time:nativetime, msg:commitmessage}, ...]
     */
    public JSONArray getHistory(String jobId) {
        JSONArray empty = new JSONArray();
        if (jobId == null || jobId.isEmpty()) {
            return empty;
        }
        try {
            return jobStoreGit.getGitLog(jobId);
        } catch (Exception e) {
            log.warn("Failed to get history for jobId " + jobId + ": " + e, e);
        }
        return empty;
    }

    /**
     * Diff the current job config with the version after a certain commit
     *
     * @param jobId    The job ID to compare
     * @param commitId The historical commit to diff against
     * @return A string describing the diff, comparable to the output of 'git diff commitid filename'
     */
    public String getDiff(String jobId, String commitId) {
        try {
            String diff = jobStoreGit.getDiff(jobId, commitId);
            if (diff != null && !diff.isEmpty()) {
                return diff;
            } else {
                return noDiffMessage;
            }
        } catch (GitAPIException g) {
            log.warn("Failed to getch git diff for jobId " + jobId + ": " + g, g);
        } catch (IOException e) {
            log.warn("Failed to find file for jobId " + jobId + ": " + e, e);
        }
        return null;
    }

    /**
     * Get the full config of a job after a certain commit
     *
     * @param jobId    The job ID to look for
     * @param commitId The historical commit to fetch from
     * @return A string describing the config after the given commit
     */
    public String fetchHistoricalConfig(String jobId, String commitId) {
        try {
            return jobStoreGit.fetchJobConfigFromHistory(jobId, commitId);
        } catch (IOException io) {
            log.warn("Failed to read stored job for " + jobId + ": " + io, io);
        } catch (GitAPIException g) {
            log.warn("Failed to fetch historical config for " + jobId + ": " + g, g);
        }
        return null;
    }

    /**
     * Returns the full config at the time when a job was deleted.
     * 
     * It's the caller's responsibility to ensure that the job was indeed deleted. If the job 
     * exists, there's no guarantee as to what this method returns.
     * 
     * @param jobId The ID of a deleted job.
     * @return <code>null</code> if unable to find commit history of the job
     * @throws Exception if any underlying git error occurred
     */
    public String getDeletedJobConfig(String jobId) throws Exception {
        try {
            String commit = jobStoreGit.getCommitHashBeforeJobDeletion(jobId);
            if (commit == null) {
                log.warn("Unable to find commit history for job {}", jobId);
                return null;
            } else {
                return jobStoreGit.fetchJobConfigFromHistory(jobId, commit);
            }
        } catch (Exception e) {
            log.warn("Failed to get deleted config for job {}: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Check a job config against the cached version that was written last
     *
     * @param jobId  The jobId of the config
     * @param config The config string for the job
     * @return True if the hashed value matches what's in the cache, meaning it should not be written again
     */
    private boolean matchesCached(String jobId, String config) {
        String hash = hashConfig(config);
        synchronized (cachedConfigHash) {
            String cached = cachedConfigHash.getIfPresent(jobId);
            if (cached != null && cached.equals(hash)) {
                // Skip the file write because the config is ~probably~ already up to date.
                return true;
            }
            cachedConfigHash.put(jobId, hash);
        }
        return false;
    }

    /**
     * Submit a new job config to be stored and committed
     *
     * @param jobId         The job id to update
     * @param author        The author who made the most recent changes
     * @param config        The latest config
     * @param commitMessage If specified, the commit message to use
     */
    public void submitConfigUpdate(String jobId, String author, String config, String commitMessage) {
        assert (jobId != null);
        config = formatConfig(config);
        synchronized (jobStoreDir) {
            if (matchesCached(jobId, config)) {
                return;
            }
            try {
                jobStoreGit.commit(jobId, author, config, commitMessage);
            } catch (Exception io) {
                log.warn("Failed to save config to file: " + io, io);
            }
        }
    }

    /**
     * Apply minor formatting to a config to ensure git is happy with the version that is written to the file
     *
     * @param config The config to format
     * @return The formatter config
     */
    private static String formatConfig(String config) {
        if (config == null) {
            return "\n";
        }
        return config.endsWith("\n") ? config : config + "\n";
    }

    /**
     * Delete the files for a particular job
     *
     * @param jobId The job id to delete
     */
    public void delete(String jobId) {
        assert (jobId != null);
        synchronized (jobStoreDir) {
            jobStoreGit.remove(jobId);
        }
    }

    /*
     * Internal function to hash job configs to avoid rewriting the same data
     */
    private static String hashConfig(String config) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return new String(md5.digest(config.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(config.hashCode());
        }
    }

}

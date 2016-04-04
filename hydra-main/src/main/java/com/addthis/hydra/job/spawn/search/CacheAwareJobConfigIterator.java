package com.addthis.hydra.job.spawn.search;

import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.spawn.SpawnState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

public class CacheAwareJobConfigIterator implements Iterator<JobIdConfigPair> {
    private static final Logger log = LoggerFactory.getLogger(CacheAwareJobConfigIterator.class);

    private final Stack<String> jobIdsNotInCache;
    private final Iterator<String> jobIdIterator;
    private final JobConfigManager jobConfigManager;

    public CacheAwareJobConfigIterator(SpawnState spawnState, JobConfigManager jobConfigManager) {
        this.jobIdIterator = spawnState.jobIdIterator();
        this.jobIdsNotInCache = new Stack<>();
        this.jobConfigManager = jobConfigManager;
    }

    @Override
    public boolean hasNext() {
       return jobIdIterator.hasNext() || jobIdsNotInCache.size() > 0;
    }

    @Override
    public JobIdConfigPair next() {
        while (jobIdIterator.hasNext()) {
            String jobId = jobIdIterator.next();
            String expandedConfig = jobConfigManager.getCachedExpandedJob(jobId);

            if (expandedConfig == null) {
                jobIdsNotInCache.add(jobId);
            } else {
                return new JobIdConfigPair(jobId, expandedConfig);
            }
        }

        if (jobIdsNotInCache.size() > 0) {
            String jobId = jobIdsNotInCache.pop();
            String expandedConfig;
            try {
                expandedConfig = jobConfigManager.getExpandedConfig(jobId);
            } catch (ExecutionException e) {
                log.warn("failed to expand job config for " + jobId, e);
                expandedConfig = "";
            }

            return new JobIdConfigPair(jobId, expandedConfig);
        }

        return null;
    }
}

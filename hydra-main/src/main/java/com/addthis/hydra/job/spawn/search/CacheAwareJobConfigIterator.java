package com.addthis.hydra.job.spawn.search;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.spawn.SpawnState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

public class CacheAwareJobConfigIterator implements Iterator<JobInfo> {
    private static final Logger log = LoggerFactory.getLogger(CacheAwareJobConfigIterator.class);

    private final Stack<Job> jobsNotInCache;
    private final Iterator<Job> jobIterator;
    private final JobConfigManager jobConfigManager;

    public CacheAwareJobConfigIterator(SpawnState spawnState, JobConfigManager jobConfigManager) {
        this.jobIterator = spawnState.jobsIterator();
        this.jobsNotInCache = new Stack<>();
        this.jobConfigManager = jobConfigManager;
    }

    @Override
    public boolean hasNext() {
       return jobIterator.hasNext() || jobsNotInCache.size() > 0;
    }

    @Override
    public JobInfo next() {
        while (jobIterator.hasNext()) {
            Job job = jobIterator.next();
            String jobId = job.getId();
            String expandedConfig = jobConfigManager.getCachedExpandedJob(jobId);


            if (expandedConfig == null) {
                jobsNotInCache.add(job);
            } else {
                return new JobInfo(jobId, expandedConfig, job.getDescription());
            }
        }

        if (jobsNotInCache.size() > 0) {
            Job job = jobsNotInCache.pop();
            String jobId = job.getId();
            String expandedConfig;
            try {
                expandedConfig = jobConfigManager.getExpandedConfig(jobId);
            } catch (ExecutionException e) {
                log.warn("failed to expand job config for " + jobId, e);
                expandedConfig = "";
            }

            return new JobInfo(jobId, expandedConfig, job.getDescription());
        }

        return null;
    }
}

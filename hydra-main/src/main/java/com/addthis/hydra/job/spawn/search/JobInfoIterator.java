package com.addthis.hydra.job.spawn.search;

import com.addthis.hydra.job.JobConfigManager;

import java.util.Iterator;

public class JobInfoIterator implements Iterator<JobInfo> {
    private final Iterator<String> jobIdIterator;
    private final JobConfigManager jobConfigManager;

    public JobInfoIterator(Iterator<String> jobIdIterator, JobConfigManager jobConfigManager) {
        this.jobIdIterator = jobIdIterator;
        this.jobConfigManager = jobConfigManager;
    }

    @Override
    public boolean hasNext() {
        return jobIdIterator.hasNext();
    }

    @Override
    public JobInfo next() {
        String id = jobIdIterator.next();
        JobInfo jobInfo = new JobInfo(id, jobConfigManager.getConfig(id), null);
        return jobInfo;
    }
}

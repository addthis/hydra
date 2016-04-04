package com.addthis.hydra.job.spawn.search;

public class JobInfo {
    protected String id;
    protected String description;
    protected String config;

    public JobInfo(String id, String config, String description) {
        this.id = id;
        this.config = config;
        this.description = description;
    }
}

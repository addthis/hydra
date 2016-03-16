package com.addthis.hydra.job.spawn;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobParameter;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.LinkedList;

public class ExpandedConfigCacheKey {
    protected String rawConfig;
    protected Collection<JobParameter> parameters;

    private static final String EMPTY_RAW_CONFIG = "";
    private static final ImmutableList<JobParameter> EMPTY_PARAMETERS = ImmutableList.of();

    public ExpandedConfigCacheKey(IJob job) {
        this(job.getConfig(), job.getParameters());
    }

    public ExpandedConfigCacheKey(String rawConfig, Collection<JobParameter> parameters) {
        this.rawConfig = rawConfig != null ? rawConfig : EMPTY_RAW_CONFIG;
        this.parameters = parameters != null ? parameters : EMPTY_PARAMETERS;
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() != ExpandedConfigCacheKey.class) {
            return false;
        }

        ExpandedConfigCacheKey other = (ExpandedConfigCacheKey) o;

        return parameters.size() == other.parameters.size() &&
                parameters.containsAll(other.parameters) &&
                rawConfig.equals(other.rawConfig);

    }

    @Override
    public int hashCode() {
        int code = rawConfig.hashCode();
        for (JobParameter jp : parameters) {
            code ^= jp.hashCode();
        }
        return code;
    }
}

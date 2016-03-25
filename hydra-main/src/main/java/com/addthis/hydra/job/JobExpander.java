package com.addthis.hydra.job;

import com.addthis.basis.util.TokenReplacerOverflowException;

import javax.annotation.Nullable;
import java.util.Collection;

public interface JobExpander {
    @Nullable
    String expandJob(String config, Collection<JobParameter> jobParams) throws FailedJobExpansionException;
}

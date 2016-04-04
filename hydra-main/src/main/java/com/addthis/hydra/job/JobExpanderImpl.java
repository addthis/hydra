package com.addthis.hydra.job;

import com.addthis.basis.util.TokenReplacerOverflowException;
import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.job.spawn.Spawn;
import com.sun.istack.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class JobExpanderImpl implements JobExpander {
    private static final Logger log = LoggerFactory.getLogger(JobExpanderImpl.class);

    private final JobEntityManager<JobMacro> jobMacroManager;
    private final AliasManager aliasManager;
    private final Spawn spawn;

    public JobExpanderImpl(Spawn spawn, JobEntityManager<JobMacro> jobMacroManager, AliasManager aliasManager) {
        this.jobMacroManager = jobMacroManager;
        this.aliasManager = aliasManager;
        this.spawn = spawn;
    }

    @Override
    @Nullable
    public String expandJob(String rawConfig, Collection<JobParameter> parameters) throws FailedJobExpansionException {
        try {
            // macro recursive expansion
            String pass0 = JobExpand.macroExpand(jobMacroManager, aliasManager, rawConfig);
            // template in params that "may" contain other macros
            String pass1 = JobExpand.macroTemplateParams(pass0, parameters);
            // macro recursive expansion again
            String pass2 = JobExpand.macroExpand(jobMacroManager, aliasManager, pass1);
            // replace remaining params not caught in pass 1
            String pass3 = JobExpand.macroTemplateParams(pass2, parameters);
            // inject job metadata from spawn
            return JobExpand.magicMacroExpand(spawn, pass3, null);
        } catch (TokenReplacerOverflowException e) {
            log.warn("failed to expand job", e);
            throw new FailedJobExpansionException();
        }
    }
}


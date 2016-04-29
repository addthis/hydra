package com.addthis.hydra.job.spawn;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SpawnUtils {
    public static Map<String, Job> getJobsMapFromSpawnState(SpawnState spawnState) {
        return ImmutableMap.copyOf(spawnState.jobs);
    }

    public static Map<String, JobMacro> getMacroMapFromMacroManager(JobEntityManager<JobMacro> jobMacroManager) {
        ImmutableMap.Builder<String, JobMacro> builder = ImmutableMap.builder();

        for (String macroName : jobMacroManager.getKeys()) {
            builder.put(macroName, jobMacroManager.getEntity(macroName));
        }

        return builder.build();
    }
}

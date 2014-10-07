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
package com.addthis.hydra.job.web;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.minion.Minion;
import com.addthis.hydra.job.spawn.Spawn;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;

public class JobRequestHandlerImpl implements JobRequestHandler {
    
    private final Spawn spawn;
    
    public JobRequestHandlerImpl(Spawn spawn) {
        this.spawn = spawn;
    }

    @Override
    public Job createOrUpdateJob(KVPairs kv, String username) throws Exception {
        String id = KVUtils.getValue(kv, "", "id", "job");
        String config = kv.getValue("config");
        String expandedConfig = null;
        String command = kv.getValue("command");
        boolean configMayHaveChanged = true;
        Job job;
        if (Strings.isNullOrEmpty(id)) {
            checkArgument(!Strings.isNullOrEmpty(command), "Parameter 'command' is missing");
            requireValidCommandParam(command);
            checkArgument(config != null, "Parameter 'config' is missing");
            expandedConfig = tryExpandJobConfigParam(config);
            job = spawn.createJob(
                    kv.getValue("owner", username),
                    kv.getIntValue("nodes", -1),
                    Splitter.on(',').omitEmptyStrings().trimResults().splitToList(kv.getValue("hosts", "")),
                    kv.getValue("minionType", Minion.defaultMinionType),
                    command);
        } else {
            job = spawn.getJob(id);
            checkArgument(job != null, "Job %s does not exist", id);
            if (config == null) {
                configMayHaveChanged = false;
                config = spawn.getJobConfig(id);
            }
            expandedConfig = tryExpandJobConfigParam(config);
            if (!Strings.isNullOrEmpty(command)) {
                requireValidCommandParam(command);
                job.setCommand(command);
            }
        }
        updateBasicSettings(kv, job);
        updateQueryConfig(kv, job);
        updateJobParameters(kv, job, expandedConfig);
        // persist update
        // XXX When this call fails the job will be left in an inconsistent state.
        // empirically, it happens rarely (e.g. no one sets replicas to an insanely large number).
        // the logic is also quite involved, so to fully fix would require a major refactoring.
        spawn.updateJob(job);
        if (configMayHaveChanged) {
            // only update config if it may have changed
            spawn.setJobConfig(job.getId(), config);
            spawn.submitConfigUpdate(job.getId(), kv.getValue("commit"));
        }
        return job;
    }

    private void requireValidCommandParam(String command) throws IllegalArgumentException {
        checkArgument(spawn.getJobCommandManager().getEntity(command) != null, "Invalid command key '%s'", command);
    }

    private String tryExpandJobConfigParam(String jobConfig) throws IllegalArgumentException {
        try {
            return JobExpand.macroExpand(spawn, jobConfig);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void updateBasicSettings(KVPairs kv, IJob job) {
        job.setOwner(kv.getValue("owner", job.getOwner()));
        job.setPriority(kv.getIntValue("priority", job.getPriority()));
        job.setDescription(kv.getValue("description", job.getDescription()));
        job.setDescription(KVUtils.getValue(kv, job.getDescription(), "description", "desc"));
        job.setOnCompleteURL(kv.getValue("onComplete", job.getOnCompleteURL()));
        job.setOnErrorURL(kv.getValue("onError", job.getOnErrorURL()));
        job.setOnCompleteTimeout(kv.getIntValue("onCompleteTimeout", job.getOnCompleteTimeout()));
        job.setOnErrorTimeout(kv.getIntValue("onErrorTimeout", job.getOnErrorTimeout()));
        job.setMaxRunTime(KVUtils.getLongValue(kv, job.getMaxRunTime(), "maxRunTime", "maxrun"));
        job.setRekickTimeout(KVUtils.getLongValue(kv, job.getRekickTimeout(), "rekickTimeout", "rekick"));
        job.setEnabled(KVUtils.getBooleanValue(kv, job.isEnabled(), "enable"));
        job.setKillSignal(kv.getValue("logkill", job.getKillSignal()));
        job.setBackups(kv.getIntValue("backups", job.getBackups()));
        job.setDailyBackups(kv.getIntValue("dailyBackups", job.getDailyBackups()));
        job.setHourlyBackups(kv.getIntValue("hourlyBackups", job.getHourlyBackups()));
        job.setWeeklyBackups(kv.getIntValue("weeklyBackups", job.getWeeklyBackups()));
        job.setMonthlyBackups(kv.getIntValue("monthlyBackups", job.getMonthlyBackups()));
        job.setReplicas(kv.getIntValue("replicas", job.getReplicas()));
        job.setReadOnlyReplicas(kv.getIntValue("readOnlyReplicas", job.getReadOnlyReplicas()));
        job.setReplicationFactor(kv.getIntValue("replicationFactor", job.getReplicationFactor()));
        job.setStomp(KVUtils.getBooleanValue(kv, job.getStomp(), "stomp"));
        job.setDontDeleteMe(KVUtils.getBooleanValue(kv, job.getDontDeleteMe(), "dontDeleteMe"));
        job.setDontAutoBalanceMe(KVUtils.getBooleanValue(kv, job.getDontAutoBalanceMe(), "dontAutoBalanceMe"));
        job.setMaxSimulRunning(kv.getIntValue("maxSimulRunning", job.getMaxSimulRunning()));
        job.setMinionType(kv.getValue("minionType", job.getMinionType()));
        job.setRetries(kv.getIntValue("retries", job.getRetries()));
    }

    private void updateQueryConfig(KVPairs kv, IJob job) {
        JobQueryConfig jqc;
        if (job.getQueryConfig() != null) {
            jqc = job.getQueryConfig().clone();
        } else {
            jqc = new JobQueryConfig();
        }
        if (kv.hasKey("qc_canQuery")) {
            jqc.setCanQuery(KVUtils.getBooleanValue(kv, true, "qc_canQuery"));
        }
        if (kv.hasKey("qc_queryTraceLevel")) {
            jqc.setQueryTraceLevel(kv.getIntValue("qc_queryTraceLevel", 0));
        }
        if (kv.hasKey("qc_consecutiveFailureThreshold")) {
            jqc.setConsecutiveFailureThreshold(kv.getIntValue("qc_consecutiveFailureThreshold", 100));
        }
        job.setQueryConfig(jqc);
    }

    private void updateJobParameters(KVPairs kv, IJob job, String expandedConfig) {
        // collect/merge parameters
        Map<String, String> setParams = new LinkedHashMap<>();
        // copy values existing in job parameters
        if (job.getParameters() != null) {
            // remove specified parameters
            for (Iterator<JobParameter> jp = job.getParameters().iterator(); jp.hasNext();) {
                JobParameter param = jp.next();
                if (kv.hasKey("rp_" + param.getName())) {
                    jp.remove();
                }
            }
            // pull in previous values
            for (JobParameter param : job.getParameters()) {
                setParams.put(param.getName(), param.getValue());
            }
        }
        /** set specified parameters */
        for (KVPair kvp : kv) {
            if (kvp.getKey().startsWith("sp_")) {
                setParams.put(kvp.getKey().substring(3), kvp.getValue());
            }
        }

        Map<String, JobParameter> macroParams = JobExpand.macroFindParameters(expandedConfig);
        List<JobParameter> newparams = new ArrayList<>(macroParams.size());
        for (JobParameter param : macroParams.values()) {
            String name = param.getName();
            String value = setParams.get(name);
            param.setValue(value);
            newparams.add(param);
        }
        job.setParameters(newparams);
    }
    
    @Override
    public boolean maybeKickJobOrTask(KVPairs kv, Job job) throws Exception {
        boolean kick = KVUtils.getBooleanValue(kv, false, "spawn");
        if (kick) {
            boolean manual = KVUtils.getBooleanValue(kv, false, "manual");
            int select = kv.getIntValue("select", -1);
            if (select >= 0) {
                spawn.startTask(job.getId(), select, true, manual, false);
            } else {
                spawn.startJob(job.getId(), manual);
            }
        }
        return kick;
    }

}

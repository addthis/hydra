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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.alert.JobAlertManager;
import com.addthis.hydra.job.auth.InsufficientPrivilegesException;
import com.addthis.hydra.job.auth.User;
import com.addthis.hydra.job.spawn.Spawn;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.IJob.DEFAULT_MINION_TYPE;
import static com.google.common.base.Preconditions.checkArgument;

public class JobRequestHandlerImpl implements JobRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(JobRequestHandlerImpl.class);

    private final Spawn spawn;
    private final JobAlertManager jobAlertManager;

    public JobRequestHandlerImpl(Spawn spawn) {
        this.spawn = spawn;
        this.jobAlertManager = spawn.getJobAlertManager();
    }

    @Override
    public Job createOrUpdateJob(KVPairs kv, String username, String token, String sudo, boolean defaults)
            throws Exception {
        User user = spawn.getPermissionsManager().authenticate(username, token);
        if (user == null) {
            throw new InsufficientPrivilegesException(username, "invalid credentials provided");
        }
        String id = KVUtils.getValue(kv, "", "id", "job");
        String config = kv.getValue("config");
        String expandedConfig;
        String command = kv.getValue("command");
        boolean configMayHaveChanged = true;
        Job job;
        if (Strings.isNullOrEmpty(id)) {
            checkArgument(!Strings.isNullOrEmpty(command), "Parameter 'command' is missing");
            requireValidCommandParam(command);
            checkArgument(config != null, "Parameter 'config' is missing");
            expandedConfig = tryExpandJobConfigParam(config);
            job = spawn.createJob(
                    kv.getValue("creator", username),
                    kv.getIntValue("nodes", -1),
                    Splitter.on(',').omitEmptyStrings().trimResults().splitToList(kv.getValue("hosts", "")),
                    kv.getValue("minionType", DEFAULT_MINION_TYPE),
                    command, defaults);
            updateOwnership(kv, job, user);
        } else {
            job = spawn.getJob(id);
            checkArgument(job != null, "Job %s does not exist", id);
            if (!spawn.getPermissionsManager().isWritable(username, token, sudo, job)) {
                log.warn("User {} (sudo = {}) had insufficient privileges to modify job {}", username,
                         (sudo != null), id);
                throw new InsufficientPrivilegesException(username, "insufficient privileges to modify job " + id);
            }
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
        updateBasicSettings(kv, job, username);
        updateQueryConfig(kv, job);
        updateJobParameters(kv, job, expandedConfig);
        updateBasicAlerts(kv, job);

        // persist update
        // XXX When this call fails the job will be left in an inconsistent state.
        // empirically, it happens rarely (e.g. no one sets replicas to an insanely large number).
        // the logic is also quite involved, so to fully fix would require a major refactoring.
        spawn.updateJob(job);
        if (configMayHaveChanged) {
            // only update config if it may have changed
            spawn.setJobConfig(job.getId(), config);
            spawn.submitConfigUpdate(job.getId(), username, kv.getValue("commit"));
        }
        return job;
    }

    private void requireValidCommandParam(String command) throws IllegalArgumentException {
        checkArgument(spawn.getJobCommandManager().getEntity(command) != null, "Invalid command key '%s'", command);
    }

    private String tryExpandJobConfigParam(String jobConfig) throws IllegalArgumentException {
        try {
            return JobExpand.macroExpand(spawn.getJobMacroManager(), spawn.getAliasManager(), jobConfig);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void updateOwnership(KVPairs kv, IJob job, User user) {
        if(Strings.isNullOrEmpty(kv.getValue("creator"))) {
            job.setCreator(user.name());
        }
        job.setOwner(user.name());
        job.setGroup(user.primaryGroup());
    }

    /**
     * Updates auto generated alerts on job. jobAlertManager may update the basicAlerts/basicPages job setting.
     */
    private void updateBasicAlerts(KVPairs kv, IJob job) {
        boolean basicAlerts = KVUtils.getBooleanValue(kv, false, "basicAlerts");
        boolean basicPages = KVUtils.getBooleanValue(kv, false, "basicPages");
        jobAlertManager.updateBasicAlerts(job, basicAlerts, basicPages);
    }

    private void updateBasicSettings(KVPairs kv, IJob job, String user) {
        job.setGroup(kv.getValue("group", job.getGroup()));
        job.setOwnerWritable(KVUtils.getBooleanValue(kv, job.isOwnerWritable(), "ownerWritable"));
        job.setGroupWritable(KVUtils.getBooleanValue(kv, job.isGroupWritable(), "groupWritable"));
        job.setWorldWritable(KVUtils.getBooleanValue(kv, job.isWorldWritable(), "worldWritable"));
        job.setOwnerExecutable(KVUtils.getBooleanValue(kv, job.isOwnerExecutable(), "ownerExecutable"));
        job.setGroupExecutable(KVUtils.getBooleanValue(kv, job.isGroupExecutable(), "groupExecutable"));
        job.setWorldExecutable(KVUtils.getBooleanValue(kv, job.isWorldExecutable(), "worldExecutable"));
        job.setLastModifiedBy(user);
        job.setLastModifiedAt(System.currentTimeMillis());
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
        job.setDailyBackups(kv.getIntValue("dailyBackups", job.getDailyBackups()));
        job.setHourlyBackups(kv.getIntValue("hourlyBackups", job.getHourlyBackups()));
        job.setWeeklyBackups(kv.getIntValue("weeklyBackups", job.getWeeklyBackups()));
        job.setMonthlyBackups(kv.getIntValue("monthlyBackups", job.getMonthlyBackups()));
        job.setReplicas(kv.getIntValue("replicas", job.getReplicas()));
        job.setDontDeleteMe(KVUtils.getBooleanValue(kv, job.getDontDeleteMe(), "dontDeleteMe"));
        job.setDontCloneMe(KVUtils.getBooleanValue(kv, job.getDontCloneMe(), "dontCloneMe"));
        job.setDontAutoBalanceMe(KVUtils.getBooleanValue(kv, job.getDontAutoBalanceMe(), "dontAutoBalanceMe"));
        job.setMaxSimulRunning(kv.getIntValue("maxSimulRunning", job.getMaxSimulRunning()));
        job.setMinionType(kv.getValue("minionType", job.getMinionType()));
        job.setAutoRetry(KVUtils.getBooleanValue(kv, job.getAutoRetry(), "autoRetry"));
    }

    private void updateQueryConfig(KVPairs kv, IJob job) {
        JobQueryConfig jqc;
        if (job.getQueryConfig() != null) {
            jqc = new JobQueryConfig(job.getQueryConfig());
        } else {
            jqc = new JobQueryConfig();
        }
        if (kv.hasKey("qc_canQuery")) {
            jqc.setCanQuery(KVUtils.getBooleanValue(kv, true, "qc_canQuery"));
        }
        job.setQueryConfig(jqc);
    }

    private void updateJobParameters(KVPairs kv, IJob job, String expandedConfig) {
        // copy existing job parameters except those marked with the rp_ prefix in the input
        Map<String, String> setParams = new LinkedHashMap<>();
        Collection<JobParameter> oldParams = job.getParameters();
        if (oldParams != null) {
            List<JobParameter> paramList = oldParams.stream().filter(p -> !kv.hasKey("rp_" + p.getName())).collect(
                    Collectors.toList());
            for (JobParameter jp: paramList) {
                // in case of key collisions stick with the first value
                if (!setParams.containsKey(jp.getName())) {
                    setParams.put(jp.getName(), jp.getValue());
                }
            }
        }
        // set specified parameters
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
            int priority = manual ? 1 : 0;
            if (select >= 0) {
                spawn.startTask(job.getId(), select, priority, false);
            } else {
                spawn.startJob(job.getId(), priority);
            }
        }
        return kick;
    }

}

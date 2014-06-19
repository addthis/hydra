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
package com.addthis.hydra.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.addthis.basis.util.JitterClock;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.spawn.JobAlert;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

// {queryconfig,config,jobtask/[n],alerts/[m]} under job uuid
public class ZnodeJob implements IJob {

    // Codable bundle of basic state
    public static class RootZnodeData implements Codec.Codable {

        @Codec.Set(codable = true)
        private JobState state;
        @Codec.Set(codable = true)
        private String creator;
        /* who last modified this job */
        @Codec.Set(codable = true)
        private String owner;
        /* purely ornamental description of this job */
        @Codec.Set(codable = true)
        private String description;
        /* key used for storing / retrieving this job */
        @Codec.Set(codable = true)
        private String id;
        /* higher means more important */
        @Codec.Set(codable = true)
        private int priority;
        /* will stomp lower pri jobs to create capacity */
        @Codec.Set(codable = true)
        private boolean stomp;
        /* Unix epoch offset of time job was created */
        @Codec.Set(codable = true)
        private Long createTime;
        /* Unix epoch offset of time job was last submitted */
        @Codec.Set(codable = true)
        private Long submitTime;
        /* Unix epoch offset of time first job node was assigned */
        @Codec.Set(codable = true)
        private Long startTime;
        /* Unix epoch offset of time last job node completed */
        @Codec.Set(codable = true)
        private Long endTime;
        /* hours between re-kicking */
        @Codec.Set(codable = true)
        private Long rekickTimeout;
        /* minutes max time to allocate to job before it's interrupted */
        @Codec.Set(codable = true)
        private Long maxRunTime;
        /* URL for spawn to call on job complete. for automating workflows */
        @Codec.Set(codable = true)
        private String onComplete;
        @Codec.Set(codable = true)
        private String onError;
        @Codec.Set(codable = true)
        private int onCompleteTimeout;
        @Codec.Set(codable = true)
        private int onErrorTimeout;
        @Codec.Set(codable = true)
        private int runCount;
        @Codec.Set(codable = true)
        private long runTime;
        @Codec.Set(codable = true)
        private String command;
        @Codec.Set(codable = true)
        private String killSignal;
        @Codec.Set(codable = true)
        private boolean enabled;
        @Codec.Set(codable = true)
        private ArrayList<JobParameter> parameters;
        @Codec.Set(codable = true)
        private int backups;
        @Codec.Set(codable = true)
        private int replicas;
        @Codec.Set(codable = true)
        private int readOnlyReplicas;
        // Unused
        @Codec.Set(codable = true)
        private int replicationFactor;
        /* restrict replicas to hosts in current job/task space */
        @Codec.Set(codable = true)
        private boolean strictReplicas;
        @Codec.Set(codable = true)
        private boolean dontAutoBalanceMe;
        @Codec.Set(codable = true)
        private boolean dontDeleteMe;
        @Codec.Set(codable = true)
        private boolean wasStopped;
        @Codec.Set(codable = true)
        private HashMap<String, String> properties;

        private JobCommand submitCommand;

        @Codec.Set(codable = true)
        private int hourlyBackups;
        @Codec.Set(codable = true)
        private int dailyBackups;
        @Codec.Set(codable = true)
        private int weeklyBackups;
        @Codec.Set(codable = true)
        private int monthlyBackups;
        @Codec.Set(codable = true)
        private int maxSimulRunning;
        @Codec.Set(codable = true)
        private String minionType;
        @Codec.Set(codable = true)
        private int retries;

        @Codec.Set(codable = true)
        private ArrayList<JobTask> tasks;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("state", state)
                    .add("creator", creator)
                    .add("owner", owner)
                    .add("description", description)
                    .add("id", id)
                    .add("priority", priority)
                    .add("stomp", stomp)
                    .add("createTime", createTime)
                    .add("submitTime", submitTime)
                    .add("startTime", startTime)
                    .add("endTime", endTime)
                    .add("rekickTimeout", rekickTimeout)
                    .add("maxRunTime", maxRunTime)
                    .add("onComplete", onComplete)
                    .add("onError", onError)
                    .add("runCount", runCount)
                    .add("runTime", runTime)
                    .add("command", command)
                    .add("killSignal", killSignal)
                    .add("enabled", enabled)
                    .add("parameters", parameters)
                    .add("backups", backups)
                    .add("replicas", replicas)
                    .add("readOnlyReplicas", readOnlyReplicas)
                    .add("strictReplicas", strictReplicas)
                    .add("dontDeleteMe", dontDeleteMe)
                    .add("dontAutoBalanceMe", dontAutoBalanceMe)
                    .add("dontDeleteMe", dontDeleteMe)
                    .add("wasStopped", wasStopped)
                    .add("submitCommand", submitCommand)
                    .add("properties", properties)
                    .add("hourlyBackups", hourlyBackups)
                    .add("dailyBackups", dailyBackups)
                    .add("weeklyBackups", weeklyBackups)
                    .add("monthlyBackups", monthlyBackups)
                    .add("tasks", tasks == null ? 0 : tasks.size())
                    .add("maxSimulRunning", maxSimulRunning)
                    .add("minionType", minionType)
                    .add("retries", retries)
                    .toString();
        }
    }

    private RootZnodeData rznData;
    private String config;
    private ArrayList<JobTask> tasks;
    private JobQueryConfig queryConfig;
    private ArrayList<JobAlert> alerts;


    public ZnodeJob(String id) {
        this(id, null);
    }

    public ZnodeJob(String id, String creator) {
        this.rznData = new RootZnodeData();
        rznData.id = id;
        rznData.creator = creator;
        rznData.createTime = JitterClock.globalTime();
        rznData.endTime = rznData.createTime;
        rznData.dontAutoBalanceMe = false;
        rznData.dontDeleteMe = false;
        config = "";
        tasks = new ArrayList<>();
        queryConfig = new JobQueryConfig();
        alerts = new ArrayList<>();
    }

    public ZnodeJob(RootZnodeData rznData, String config, JobQueryConfig queryConfig, List<JobAlert> alerts) {
        this(rznData, config, queryConfig, alerts, rznData.tasks);
    }


    public ZnodeJob(RootZnodeData rznData, String config, JobQueryConfig queryConfig, List<JobAlert> alerts, List<JobTask> tasks) {
        this.rznData = rznData;
        this.config = config != null ? config : "";
        this.queryConfig = queryConfig != null ? queryConfig : new JobQueryConfig();
        this.tasks = tasks != null ? Lists.newArrayList(tasks) : new ArrayList<JobTask>();
        this.alerts = (alerts != null ? Lists.newArrayList(alerts) : new ArrayList<JobAlert>());
    }

    public ZnodeJob(IJob job) {
        this.rznData = new RootZnodeData();

        rznData.state = job.getState();
        rznData.creator = job.getCreator();
        rznData.owner = job.getOwner();
        rznData.description = job.getDescription();
        rznData.id = job.getId();
        rznData.priority = job.getPriority();
        rznData.stomp = job.getStomp();
        rznData.createTime = job.getCreateTime();
        rznData.submitTime = job.getSubmitTime();
        rznData.startTime = job.getStartTime();
        rznData.endTime = job.getEndTime();
        rznData.rekickTimeout = job.getRekickTimeout();
        rznData.maxRunTime = job.getMaxRunTime();
        rznData.onComplete = job.getOnCompleteURL();
        rznData.onError = job.getOnErrorURL();
        rznData.onCompleteTimeout = job.getOnCompleteTimeout();
        rznData.onErrorTimeout = job.getOnErrorTimeout();
        rznData.runCount = job.getRunCount();
        rznData.runTime = job.getRunTime();
        rznData.command = job.getCommand();
        rznData.killSignal = job.getKillSignal();
        rznData.enabled = job.isEnabled();
        rznData.parameters = job.getParameters() == null ? new ArrayList<JobParameter>() : Lists.newArrayList(job.getParameters());
        rznData.backups = job.getBackups();
        rznData.replicas = job.getReplicas();
        rznData.readOnlyReplicas = job.getReadOnlyReplicas();
        rznData.replicationFactor = job.getReplicationFactor();
        rznData.strictReplicas = job.getStrictReplicas();
        rznData.dontAutoBalanceMe = job.getDontAutoBalanceMe();
        rznData.dontDeleteMe = job.getDontDeleteMe();
        rznData.wasStopped = job.getWasStopped();
        rznData.submitCommand = job.getSubmitCommand();
        rznData.properties = job.getProperties();
        rznData.hourlyBackups = job.getHourlyBackups();
        rznData.dailyBackups = job.getDailyBackups();
        rznData.weeklyBackups = job.getWeeklyBackups();
        rznData.monthlyBackups = job.getMonthlyBackups();
        rznData.tasks = new ArrayList<>(job.getCopyOfTasks());
        rznData.maxSimulRunning = job.getMaxSimulRunning();
        rznData.minionType = job.getMinionType();
        rznData.retries = job.getRetries();

        setTasks(job.getCopyOfTasks());
        setAlerts(job.getAlerts());

        this.config = job.getConfig();
        queryConfig = job.getQueryConfig();
    }

    public RootZnodeData getRootData() {
        return rznData;
    }


    // ----- Interface
    public String getId() {
        return rznData.id;
    }

    public String getOwner() {
        return rznData.owner;
    }

    public void setOwner(String owner) {
        this.rznData.owner = owner;
    }

    public String getCreator() {
        return rznData.creator;
    }

    public long getCreateTime() {
        return rznData.createTime;
    }

    public String getDescription() {
        return rznData.description;
    }

    public void setDescription(String description) {
        this.rznData.description = description;
    }

    public String getCommand() {
        return rznData.command;
    }

    public void setCommand(String command) {
        this.rznData.command = command;
    }

    public String getKillSignal() {
        return rznData.killSignal;
    }

    public void setKillSignal(String killSignal) {
        this.rznData.killSignal = killSignal;
    }

    public int getPriority() {
        return rznData.priority;
    }

    public void setPriority(int priority) {
        this.rznData.priority = priority;
    }

    public boolean getStomp() {
        return rznData.stomp;
    }

    public void setStomp(boolean stomp) {
        this.rznData.stomp = stomp;
    }

    public Long getSubmitTime() {
        return rznData.submitTime;
    }

    public void setSubmitTime(long submitTime) {
        this.rznData.submitTime = submitTime;
    }

    public Long getStartTime() {
        return rznData.startTime;
    }

    public void setStartTime(Long startTime) {
        this.rznData.startTime = startTime;
    }


    public Long getEndTime() {
        return rznData.endTime;
    }

    public void setEndTime(Long endTime) {
        this.rznData.endTime = endTime;
    }

    public Long getRekickTimeout() {
        return rznData.rekickTimeout;
    }

    public void setRekickTimeout(Long rekick) {
        rznData.rekickTimeout = rekick != null && rekick > 0 ? rekick : null;
    }

    public Long getMaxRunTime() {
        return rznData.maxRunTime;
    }

    public void setMaxRunTime(Long maxRunTime) {
        this.rznData.maxRunTime = maxRunTime;
    }

    public boolean isEnabled() {
        return rznData.enabled;
    }

    public boolean setEnabled(boolean enabled) {
        rznData.enabled = true;
        return isEnabled();
    }

    public Collection<JobParameter> getParameters() {
        return rznData.parameters;
    }

    public void setParameters(Collection<JobParameter> parameters) {
        this.rznData.parameters = new ArrayList<>(parameters.size());
        this.rznData.parameters.addAll(parameters);
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getOnCompleteURL() {
        return rznData.onComplete;
    }

    public void setOnCompleteURL(String url) {
        this.rznData.onComplete = url;
    }

    public String getOnErrorURL() {
        return rznData.onError;
    }

    public void setOnErrorURL(String url) {
        this.rznData.onError = url;
    }

    public int getOnCompleteTimeout() { return rznData.onCompleteTimeout; }

    public void setOnCompleteTimeout(int timeout) { this.rznData.onCompleteTimeout = timeout; }

    public int getOnErrorTimeout() { return rznData.onErrorTimeout; }

    public void setOnErrorTimeout(int timeout) { this.rznData.onErrorTimeout = timeout; }

    public int getBackups() {
        return rznData.backups;
    }

    public void setBackups(int backups) {
        this.rznData.backups = backups;
    }

    public int getReplicas() {
        return rznData.replicas;
    }

    public void setReplicas(int replicas) {
        this.rznData.replicas = replicas;
    }

    @Override
    public int getReadOnlyReplicas() {
        return this.rznData.readOnlyReplicas;
    }

    @Override
    public void setReadOnlyReplicas(int readOnlyReplicas) {
        this.rznData.readOnlyReplicas = readOnlyReplicas;
    }

    public int getRunCount() {
        return rznData.runCount;
    }

    public int incrementRunCount() {
        return ++rznData.runCount;
    }

    public long getRunTime() {
        return rznData.runTime;
    }

    public JobState getState() {
        return rznData.state;
    }

    public boolean setState(JobState state) {
        if (getState().canTransition(state)) {
            rznData.state = state;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void setAlerts(List<JobAlert> alerts) {
        if (alerts != null) {
            this.alerts = Lists.newArrayList(alerts);
        }
    }

    @Override
    public List<JobAlert> getAlerts() {
        return this.alerts;
    }

    public JobTask getTask(int id) {
        if (tasks == null) {
            return null;
        }
        for (JobTask node : tasks) {
            if (node.getTaskID() == id) {
                return node;
            }
        }
        return null;
    }

    public List<JobTask> getCopyOfTasks() {
        if (tasks == null) {
            return null;
        }
        return ImmutableList.copyOf(tasks);
    }

    public void addTask(JobTask task) {
        tasks.add(task);
    }

    public void setTasks(List<JobTask> tasks) {
        if (tasks != null) {
            this.tasks = Lists.newArrayList(tasks);
        }
    }

    public JobQueryConfig getQueryConfig() {
        return queryConfig;
    }

    public void setQueryConfig(JobQueryConfig queryConfig) {
        this.queryConfig = queryConfig;
    }

    public JobCommand getSubmitCommand() {
        return rznData.submitCommand;
    }

    public void setSubmitCommand(JobCommand submitCommand) {
        this.rznData.submitCommand = submitCommand;
    }

    public boolean getStrictReplicas() {
        return rznData.strictReplicas;
    }

    public void setStrictReplicas(boolean strictReplicas) {
        this.rznData.strictReplicas = strictReplicas;
    }

    public HashMap<String, String> getProperties() {
        return rznData.properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        rznData.properties = properties;
    }

    // Should this really be here?
    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("RootZnodeData", getRootData())
                .add("config", getConfig())
                .add("job-query-config", getQueryConfig())
                .add("tasks", getCopyOfTasks())
                .toString();
    }

    @Override
    public int compareTo(IJob o) {
        return getSubmitTime() > o.getSubmitTime() ? 1 : -1;
    }

    @Override
    public int getReplicationFactor() {
        return rznData.replicationFactor;
    }

    @Override
    public void setReplicationFactor(int replicationFactor) {
        rznData.replicationFactor = replicationFactor;
    }

    @Override
    public boolean getDontAutoBalanceMe() {
        return rznData.dontAutoBalanceMe;
    }

    @Override
    public void setDontAutoBalanceMe(boolean canAutoBalance) {
        rznData.dontAutoBalanceMe = canAutoBalance;
    }

    @Override
    public boolean getDontDeleteMe() {
        return rznData.dontDeleteMe;
    }

    @Override
    public void setDontDeleteMe(boolean dontDeleteMe) {
        rznData.dontDeleteMe = dontDeleteMe;
    }

    @Override
    public boolean getWasStopped() {
        return rznData.wasStopped;
    }

    @Override
    public void setWasStopped(boolean wasStopped) {
        rznData.wasStopped = wasStopped;
    }

    @Override
    public int getMaxSimulRunning() {
        return rznData.maxSimulRunning;
    }

    @Override
    public void setMaxSimulRunning(int maxSimulRunning) {
        rznData.maxSimulRunning = maxSimulRunning;
    }

    public int getHourlyBackups() {
        return rznData.hourlyBackups;
    }

    public int getDailyBackups() {
        return rznData.dailyBackups;
    }

    public int getWeeklyBackups() {
        return rznData.weeklyBackups;
    }

    public int getMonthlyBackups() {
        return rznData.monthlyBackups;
    }

    public void setHourlyBackups(int hourlyBackups) {
        rznData.hourlyBackups = hourlyBackups;
    }

    public void setDailyBackups(int dailyBackups) {
        rznData.dailyBackups = dailyBackups;
    }

    public void setWeeklyBackups(int weeklyBackups) {
        rznData.weeklyBackups = weeklyBackups;
    }

    public void setMonthlyBackups(int monthlyBackups) {
        rznData.monthlyBackups = monthlyBackups;
    }

    public ArrayList<JobTask> getTasks() {
        return rznData.tasks;
    }

    public void setTasks(ArrayList<JobTask> tasks) {
        rznData.tasks = tasks;
    }

    public String getMinionType() {
        return rznData.minionType;
    }

    public void setMinionType(String minionType) {
        rznData.minionType = minionType;
    }

    @Override
    public int getRetries() {
        return rznData.retries;
    }

    @Override
    public void setRetries(int retries) {
        rznData.retries = retries;
    }


}

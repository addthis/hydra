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
import java.util.List;

import com.addthis.basis.util.JitterClock;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// {queryconfig,config,jobtask/[n],alerts/[m]} under job uuid
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ZnodeJob implements IJob {

    // Codable bundle of basic state
    @JsonIgnoreProperties({"stomp", "killSignal", "readOnlyReplicas", "strictReplicas", "hadMoreData",
                           "replicationFactor", "alerts", "properties", "backups", "submitCommand", "retries",
                           "group", "ownerWritable", "groupWritable", "worldWritable",
                           "ownerExecutable", "groupExecutable", "worldExecutable", "lastModifiedBy", "lastModifiedAt"})
    public static class RootZnodeData {

        @FieldConfig private JobState state;
        /* creator of the job */
        @FieldConfig private String creator;
        /* owner of the job */
        @FieldConfig private String owner;
        /* group of the job */
        @FieldConfig private String group;
        /* can the owner modify the job */
        @FieldConfig private boolean ownerWritable;
        /* can the group modify the job */
        @FieldConfig private boolean groupWritable;
        /* can the world modify the job */
        @FieldConfig private boolean worldWritable;
        /* can the owner start/stop the job */
        @FieldConfig private boolean ownerExecutable;
        /* can the group start/stop the job */
        @FieldConfig private boolean groupExecutable;
        /* can the world start/stop the job */
        @FieldConfig private boolean worldExecutable;
        /* user who last modified the job */
        @FieldConfig private String lastModifiedBy;
        /* last modification time */
        @FieldConfig private long lastModifiedAt;
        /* purely ornamental description of this job */
        @FieldConfig private String description;
        /* key used for storing / retrieving this job */
        @FieldConfig private String id;
        /* higher means more important */
        @FieldConfig private int priority;
        /* Unix epoch offset of time job was created */
        @FieldConfig private Long createTime;
        /* Unix epoch offset of time job was last submitted */
        @FieldConfig private Long submitTime;
        /* Unix epoch offset of time first job node was assigned */
        @FieldConfig private Long startTime;
        /* Unix epoch offset of time last job node completed */
        @FieldConfig private Long endTime;
        /* hours between re-kicking */
        @FieldConfig private Long rekickTimeout;
        /* minutes max time to allocate to job before it's interrupted */
        @FieldConfig private Long maxRunTime;
        /* URL for spawn to call on job complete. for automating workflows */
        @FieldConfig private String onComplete;
        @FieldConfig private String onError;
        @FieldConfig private int onCompleteTimeout;
        @FieldConfig private int onErrorTimeout;
        @FieldConfig private int runCount;
        @FieldConfig private long runTime;
        @FieldConfig private String command;
        @FieldConfig private boolean enabled;
        @FieldConfig private ArrayList<JobParameter> parameters;
        @FieldConfig private int replicas;
        @FieldConfig private boolean dontAutoBalanceMe;
        @FieldConfig private boolean dontDeleteMe;
        @FieldConfig private boolean dontCloneMe;
        @FieldConfig private boolean wasStopped;

        @FieldConfig private int hourlyBackups;
        @FieldConfig private int dailyBackups;
        @FieldConfig private int weeklyBackups;
        @FieldConfig private int monthlyBackups;
        @FieldConfig private int maxSimulRunning;
        @FieldConfig private String minionType;
        @FieldConfig private boolean autoRetry;

        @FieldConfig private ArrayList<JobTask> tasks;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("state", state)
                    .add("creator", creator)
                    .add("owner", owner)
                    .add("group", group)
                    .add("ownerWritable", ownerWritable)
                    .add("groupWritable", groupWritable)
                    .add("worldWritable", worldWritable)
                    .add("ownerExecutable", ownerExecutable)
                    .add("groupExecutable", groupExecutable)
                    .add("worldExecutable", worldExecutable)
                    .add("lastModifiedBy", lastModifiedBy)
                    .add("lastModifiedAt", lastModifiedAt)
                    .add("description", description)
                    .add("id", id)
                    .add("priority", priority)
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
                    .add("enabled", enabled)
                    .add("parameters", parameters)
                    .add("replicas", replicas)
                    .add("dontDeleteMe", dontDeleteMe)
                    .add("dontCloneMe", dontCloneMe)
                    .add("dontAutoBalanceMe", dontAutoBalanceMe)
                    .add("wasStopped", wasStopped)
                    .add("hourlyBackups", hourlyBackups)
                    .add("dailyBackups", dailyBackups)
                    .add("weeklyBackups", weeklyBackups)
                    .add("monthlyBackups", monthlyBackups)
                    .add("tasks", tasks == null ? 0 : tasks.size())
                    .add("maxSimulRunning", maxSimulRunning)
                    .add("minionType", minionType)
                    .add("autoRetry", autoRetry)
                    .toString();
        }
    }

    private RootZnodeData rznData;
    private String config;
    private ArrayList<JobTask> tasks;
    private JobQueryConfig queryConfig;


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
        rznData.dontCloneMe = false;
        config = "";
        tasks = new ArrayList<>();
        queryConfig = new JobQueryConfig();
    }

    public ZnodeJob(RootZnodeData rznData, String config, JobQueryConfig queryConfig) {
        this(rznData, config, queryConfig, rznData.tasks);
    }

    public ZnodeJob(RootZnodeData rznData, String config, JobQueryConfig queryConfig, List<JobTask> tasks) {
        this.rznData = rznData;
        this.config = config != null ? config : "";
        this.queryConfig = queryConfig != null ? queryConfig : new JobQueryConfig();
        this.tasks = tasks != null ? Lists.newArrayList(tasks) : new ArrayList<>();
    }

    public ZnodeJob(IJob job) {
        this.rznData = new RootZnodeData();

        rznData.state = job.getState();
        rznData.creator = job.getCreator();
        rznData.owner = job.getOwner();
        rznData.group = job.getGroup();
        rznData.ownerWritable = job.isOwnerWritable();
        rznData.groupWritable = job.isGroupWritable();
        rznData.worldWritable = job.isWorldWritable();
        rznData.ownerExecutable = job.isOwnerExecutable();
        rznData.groupExecutable = job.isGroupExecutable();
        rznData.worldExecutable = job.isWorldExecutable();
        rznData.lastModifiedAt = job.lastModifiedAt();
        rznData.lastModifiedBy = job.lastModifiedBy();
        rznData.description = job.getDescription();
        rznData.id = job.getId();
        rznData.priority = job.getPriority();
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
        rznData.enabled = job.isEnabled();
        rznData.parameters = job.getParameters() == null ? new ArrayList<>() : Lists.newArrayList(job.getParameters());
        rznData.replicas = job.getReplicas();
        rznData.dontAutoBalanceMe = job.getDontAutoBalanceMe();
        rznData.dontDeleteMe = job.getDontDeleteMe();
        rznData.dontCloneMe = job.getDontCloneMe();
        rznData.wasStopped = job.getWasStopped();
        rznData.hourlyBackups = job.getHourlyBackups();
        rznData.dailyBackups = job.getDailyBackups();
        rznData.weeklyBackups = job.getWeeklyBackups();
        rznData.monthlyBackups = job.getMonthlyBackups();
        rznData.tasks = new ArrayList<>(job.getCopyOfTasks());
        rznData.maxSimulRunning = job.getMaxSimulRunning();
        rznData.minionType = job.getMinionType();
        rznData.autoRetry = job.getAutoRetry();

        setTasks(job.getCopyOfTasks());

        this.config = job.getConfig();
        queryConfig = job.getQueryConfig();
    }

    public RootZnodeData getRootData() {
        return rznData;
    }


    // ----- Interface
    @Override public String getId() {
        return rznData.id;
    }

    @Override public String getCreator() {
        return rznData.creator;
    }

    @Override public String getOwner() {
        return rznData.owner;
    }

    @Override public void setOwner(String owner) {
        this.rznData.owner = owner;
    }

    @Override public String getGroup() {
        return rznData.group;
    }

    @Override public void setGroup(String group) {
        this.rznData.group = group;
    }

    @Override
    public boolean isOwnerWritable() {
        return rznData.ownerWritable;
    }

    @Override
    public void setOwnerWritable(boolean ownerWritable) {
        this.rznData.ownerWritable = ownerWritable;
    }

    @Override
    public boolean isGroupWritable() {
        return rznData.groupWritable;
    }

    @Override
    public void setGroupWritable(boolean groupWritable) {
        this.rznData.groupWritable = groupWritable;
    }

    @Override
    public boolean isWorldWritable() {
        return rznData.worldWritable;
    }

    @Override
    public void setWorldWritable(boolean worldWritable) {
        this.rznData.worldWritable = worldWritable;
    }

    @Override
    public boolean isOwnerExecutable() {
        return rznData.ownerExecutable;
    }

    @Override
    public void setOwnerExecutable(boolean ownerExecutable) {
        this.rznData.ownerExecutable = ownerExecutable;
    }

    @Override
    public boolean isGroupExecutable() {
        return rznData.groupExecutable;
    }

    @Override
    public void setGroupExecutable(boolean groupExecutable) {
        this.rznData.groupExecutable = groupExecutable;
    }

    @Override
    public boolean isWorldExecutable() {
        return rznData.worldExecutable;
    }

    @Override
    public void setWorldExecutable(boolean worldExecutable) {
        this.rznData.worldExecutable = worldExecutable;
    }

    @Override
    public String lastModifiedBy() {
        return rznData.lastModifiedBy;
    }

    @Override
    public void setLastModifiedBy(String user) {
        this.rznData.lastModifiedBy = user;
    }

    @Override
    public long lastModifiedAt() {
        return rznData.lastModifiedAt;
    }

    @Override
    public void setLastModifiedAt(long time) {
        this.rznData.lastModifiedAt = time;
    }

    @Override public long getCreateTime() {
        return rznData.createTime;
    }

    @Override public String getDescription() {
        return rznData.description;
    }

    @Override public void setDescription(String description) {
        this.rznData.description = description;
    }

    @Override public String getCommand() {
        return rznData.command;
    }

    @Override public void setCommand(String command) {
        this.rznData.command = command;
    }

    @Override public int getPriority() {
        return rznData.priority;
    }

    @Override public void setPriority(int priority) {
        this.rznData.priority = priority;
    }

    @Override public Long getSubmitTime() {
        return rznData.submitTime;
    }

    @Override public void setSubmitTime(long submitTime) {
        this.rznData.submitTime = submitTime;
    }

    @Override public Long getStartTime() {
        return rznData.startTime;
    }

    @Override public void setStartTime(Long startTime) {
        this.rznData.startTime = startTime;
    }


    @Override public Long getEndTime() {
        return rznData.endTime;
    }

    @Override public void setEndTime(Long endTime) {
        this.rznData.endTime = endTime;
    }

    @Override public Long getRekickTimeout() {
        return rznData.rekickTimeout;
    }

    @Override public void setRekickTimeout(Long rekick) {
        rznData.rekickTimeout = rekick != null && rekick > 0 ? rekick : null;
    }

    @Override public Long getMaxRunTime() {
        return rznData.maxRunTime;
    }

    @Override public void setMaxRunTime(Long maxRunTime) {
        this.rznData.maxRunTime = maxRunTime;
    }

    @Override public boolean isEnabled() {
        return rznData.enabled;
    }

    @Override public boolean setEnabled(boolean enabled) {
        rznData.enabled = true;
        return isEnabled();
    }

    @Override public Collection<JobParameter> getParameters() {
        return rznData.parameters;
    }

    @Override public void setParameters(Collection<JobParameter> parameters) {
        this.rznData.parameters = new ArrayList<>(parameters.size());
        this.rznData.parameters.addAll(parameters);
    }

    @Override public String getConfig() {
        return config;
    }

    @Override public void setConfig(String config) {
        this.config = config;
    }

    @Override public String getOnCompleteURL() {
        return rznData.onComplete;
    }

    @Override public void setOnCompleteURL(String url) {
        this.rznData.onComplete = url;
    }

    @Override public String getOnErrorURL() {
        return rznData.onError;
    }

    @Override public void setOnErrorURL(String url) {
        this.rznData.onError = url;
    }

    @Override public int getOnCompleteTimeout() { return rznData.onCompleteTimeout; }

    @Override public void setOnCompleteTimeout(int timeout) { this.rznData.onCompleteTimeout = timeout; }

    @Override public int getOnErrorTimeout() { return rznData.onErrorTimeout; }

    @Override public void setOnErrorTimeout(int timeout) { this.rznData.onErrorTimeout = timeout; }

    @Override public int getReplicas() {
        return rznData.replicas;
    }

    @Override public void setReplicas(int replicas) {
        this.rznData.replicas = replicas;
    }

    @Override public int getRunCount() {
        return rznData.runCount;
    }

    @Override public int incrementRunCount() {
        return ++rznData.runCount;
    }

    @Override public long getRunTime() {
        return rznData.runTime;
    }

    @Override public JobState getState() {
        return rznData.state;
    }

    @Override public boolean setState(JobState state) {
        if (getState().canTransition(state)) {
            rznData.state = state;
            return true;
        } else {
            return false;
        }
    }

    @Override public JobTask getTask(int id) {
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

    @Override public List<JobTask> getCopyOfTasks() {
        if (tasks == null) {
            return null;
        }
        return ImmutableList.copyOf(tasks);
    }

    @Override public void addTask(JobTask task) {
        tasks.add(task);
    }

    @Override public void setTasks(List<JobTask> tasks) {
        if (tasks != null) {
            this.tasks = Lists.newArrayList(tasks);
        }
    }

    @Override public JobQueryConfig getQueryConfig() {
        return queryConfig;
    }

    @Override public void setQueryConfig(JobQueryConfig queryConfig) {
        this.queryConfig = queryConfig;
    }

    // Should this really be here?
    @Override public JSONObject toJSON() throws Exception {
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
    public boolean getDontCloneMe() {
        return rznData.dontCloneMe;
    }

    @Override
    public void setDontCloneMe(boolean dontCloneMe) {
        rznData.dontCloneMe = dontCloneMe;
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

    @Override public int getHourlyBackups() {
        return rznData.hourlyBackups;
    }

    @Override public int getDailyBackups() {
        return rznData.dailyBackups;
    }

    @Override public int getWeeklyBackups() {
        return rznData.weeklyBackups;
    }

    @Override public int getMonthlyBackups() {
        return rznData.monthlyBackups;
    }

    @Override public void setHourlyBackups(int hourlyBackups) {
        rznData.hourlyBackups = hourlyBackups;
    }

    @Override public void setDailyBackups(int dailyBackups) {
        rznData.dailyBackups = dailyBackups;
    }

    @Override public void setWeeklyBackups(int weeklyBackups) {
        rznData.weeklyBackups = weeklyBackups;
    }

    @Override public void setMonthlyBackups(int monthlyBackups) {
        rznData.monthlyBackups = monthlyBackups;
    }

    public ArrayList<JobTask> getTasks() {
        return rznData.tasks;
    }

    public void setTasks(ArrayList<JobTask> tasks) {
        rznData.tasks = tasks;
    }

    @Override public String getMinionType() {
        return rznData.minionType;
    }

    @Override public void setMinionType(String minionType) {
        rznData.minionType = minionType;
    }

    @Override
    public boolean getAutoRetry() {
        return rznData.autoRetry;
    }

    @Override
    public void setAutoRetry(boolean autoRetry) {
        rznData.autoRetry = autoRetry;
    }


}

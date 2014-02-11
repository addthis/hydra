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
import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.Codec.Codable;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.spawn.JobAlert;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.slf4j.Logger;


import org.slf4j.LoggerFactory;
/**
 * for job submission and tracking
 * IJob that keeps everything in gone Codable Object graph
 */
public final class Job implements IJob, Codable {

    private static Logger log = LoggerFactory.getLogger(Job.class);

    /* what is the state of this job */
    @Codec.Set(codable = true)
    private int state;
    /* how many tasks are active */
    @Codec.Set(codable = true)
    private int countActiveTasks;
    /* who created this job */
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
    /* list of nodes and their state */
    @Codec.Set(codable = true)
    private ArrayList<JobTask> nodes;
    /* JSON configuration url -- only read at submit time if conf empty */
    @Codec.Set(codable = true)
    private String config;
    /* alerts on the job */
    @Codec.Set(codable = true)
    private List<JobAlert> alerts;
    /* URL for spawn to call on job complete. for automating workflows */
    @Codec.Set(codable = true)
    private String onComplete;
    @Codec.Set(codable = true)
    private String onError;
    @Codec.Set(codable = true)
    private int runCount;
    @Codec.Set(codable = true)
    private long runTime;
    @Codec.Set(codable = true)
    private String command;
    @Codec.Set(codable = true)
    private String killSignal;
    @Codec.Set(codable = true)
    private boolean disabled;
    @Codec.Set(codable = true)
    private ArrayList<JobParameter> parameters;
    @Codec.Set(codable = true)
    private int backups; //Leaving in for cutover
    @Codec.Set(codable = true)
    private Integer hourlyBackups;
    @Codec.Set(codable = true)
    private Integer dailyBackups;
    @Codec.Set(codable = true)
    private Integer weeklyBackups;
    @Codec.Set(codable = true)
    private Integer monthlyBackups;
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
    private HashMap<String, String> properties;
    @Codec.Set(codable = true)
    private boolean hadMoreData;
    @Codec.Set(codable = true)
    private boolean wasStopped;
    @Codec.Set(codable = true)
    private int maxSimulRunning;
    @Codec.Set(codable = true)
    private String minionType;
    @Codec.Set(codable = true)
    private int retries;

    // Query Config, eventually the job class needs to be teased apart.
    @Codec.Set(codable = true)
    private JobQueryConfig queryConfig;

    private JobCommand submitCommand;

    // If all errored tasks from an errored job are resolved and the job has started within this cutoff, automatically enable the job. Default is 3 days.
    private static final long AUTO_ENABLE_CUTOFF = Parameter.longValue("job.enable.cutoff", 1000 * 60 * 60 * 24 * 3);

    // For codec only
    public Job() {
    }

    public Job(String id) {
        this(id, null);
    }

    public Job(String id, String creator) {
        this.id = id;
        this.creator = creator;
        this.createTime = JitterClock.globalTime();
        this.endTime = createTime;
        this.dontAutoBalanceMe = false;
        this.dontDeleteMe = false;
        this.config = "";
        this.queryConfig = new JobQueryConfig();
    }

    public Job(IJob job) {
        this.id = job.getId();
        this.setState(job.getState());
        this.creator = job.getCreator();
        this.owner = job.getOwner();
        this.description = job.getDescription();
        this.priority = job.getPriority();
        this.stomp = job.getStomp();
        this.replicationFactor = job.getReplicationFactor();
        this.createTime = job.getCreateTime();
        this.submitTime = job.getSubmitTime();
        this.startTime = job.getStartTime();
        this.endTime = job.getEndTime();
        this.rekickTimeout = job.getRekickTimeout();
        this.maxRunTime = job.getMaxRunTime();
        this.setTasks(job.getCopyOfTasks());
        setAlerts(job.getAlerts());
        recountActiveTasks();
        this.config = job.getConfig();
        this.onComplete = job.getOnCompleteURL();
        this.onError = job.getOnErrorURL();
        this.runCount = job.getRunCount();
        this.runTime = job.getRunTime();
        this.command = job.getCommand();
        this.killSignal = job.getKillSignal();
        this.parameters = job.getParameters() != null ? Lists.newArrayList(job.getParameters()) : null;
        this.backups = job.getBackups();
        this.hourlyBackups = job.getHourlyBackups();
        // Cutover logic: set dailyBackups to legacyBackups * 2
        if (job.getDailyBackups() == 0 && job.getBackups() != 0) {
            this.dailyBackups = 2 * this.backups;
        } else {
            this.dailyBackups = job.getDailyBackups();
        }
        this.weeklyBackups = job.getWeeklyBackups();
        this.monthlyBackups = job.getMonthlyBackups();
        this.retries = job.getRetries();
        this.replicas = job.getReplicas();
        this.readOnlyReplicas = job.getReadOnlyReplicas();
        this.strictReplicas = job.getStrictReplicas();
        this.queryConfig = job.getQueryConfig();
        this.submitCommand = job.getSubmitCommand();
        this.dontAutoBalanceMe = job.getDontAutoBalanceMe();
        this.dontDeleteMe = job.getDontDeleteMe();
        this.maxSimulRunning = job.getMaxSimulRunning();
        this.minionType = job.getMinionType();
        this.wasStopped = job.getWasStopped();
        this.properties = job.getProperties();
        setEnabled(job.isEnabled());
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String getCreator() {
        return creator;
    }

    @Override
    public long getCreateTime() {
        return createTime;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getCommand() {
        return command;
    }

    @Override
    public void setCommand(String command) {
        this.command = command;
    }

    @Override
    public String getKillSignal() {
        return killSignal;
    }

    @Override
    public void setKillSignal(String killSignal) {
        this.killSignal = killSignal;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public boolean getStomp() {
        return stomp;
    }

    @Override
    public void setStomp(boolean stomp) {
        this.stomp = stomp;
    }

    @Override
    public Long getSubmitTime() {
        return submitTime;
    }

    @Override
    public void setSubmitTime(long submitTime) {
        this.submitTime = submitTime;
    }

    @Override
    public Long getStartTime() {
        return startTime;
    }

    @Override
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @Override
    public Long getEndTime() {
        return endTime;
    }

    @Override
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public void setFinishTime(long finishTime) {
        if (startTime != null) {
            runTime += finishTime - startTime;
        }
        endTime = finishTime;
    }

    @Override
    public Long getRekickTimeout() {
        return rekickTimeout;
    }

    @Override
    public void setRekickTimeout(Long rekick) {
        rekickTimeout = rekick != null && rekick > 0 ? rekick : null;
    }

    @Override
    public Long getMaxRunTime() {
        return maxRunTime;
    }

    @Override
    public void setMaxRunTime(Long maxRunTime) {
        this.maxRunTime = maxRunTime;
    }

    @Override
    public boolean isEnabled() {
        return !disabled;
    }

    @Override
    public boolean setEnabled(boolean enabled) {
        if (enabled == disabled) {
            disabled = !enabled;
            // Determine new states
            if (enabled && state == JobState.ERROR.getValue()) {
                for (JobTask task : getCopyOfTasks()) {
                    JobTaskState state = task.getState();
                    task.setErrorCode(0);
                    task.setPreFailErrorCode(0);
                    if (state == JobTaskState.ERROR || state == JobTaskState.DISK_FULL) {
                        setTaskState(task, JobTaskState.IDLE, true);
                    }
                }
                calculateJobState(true);
            } else if (enabled && state == JobState.DEGRADED.getValue()) {
                // Clear degraded state by recalculating
                calculateJobState(true);
            }
            return true;
        }
        return false;
    }

    @Override
    public Collection<JobParameter> getParameters() {
        return parameters;
    }

    @Override
    public void setParameters(Collection<JobParameter> parameters) {
        if (parameters != null) {
            this.parameters = new ArrayList<>(parameters.size());
            this.parameters.addAll(parameters);
        } else {
            this.parameters = null;
        }
    }

    @Override
    public String getConfig() {
        return config;
    }

    @Override
    public void setConfig(String config) {
        this.config = config;
    }

    @Override
    public String getOnCompleteURL() {
        return onComplete;
    }

    @Override
    public void setOnCompleteURL(String url) {
        this.onComplete = url;
    }

    @Override
    public String getOnErrorURL() {
        return onError;
    }

    @Override
    public void setOnErrorURL(String url) {
        this.onError = url;
    }

    @Override
    public int getBackups() {
        return backups;
    }

    @Override
    public void setBackups(int backups) {
        this.backups = backups;
    }

    @Override
    public int getReplicas() {
        return replicas;
    }

    @Override
    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Override
    public int getReadOnlyReplicas() {
        return readOnlyReplicas;
    }

    @Override
    public void setReadOnlyReplicas(int readOnlyReplicas) {
        this.readOnlyReplicas = readOnlyReplicas;
    }

    @Override
    public int getRunCount() {
        return runCount;
    }

    @Override
    public int incrementRunCount() {
        return ++runCount;
    }

    @Override
    public long getRunTime() {
        return runTime;
    }

    @Override
    public JobState getState() {
        JobState jobState = JobState.makeState(state);
        return jobState == null ? JobState.UNKNOWN : jobState;
    }

    @Override
    public void setAlerts(List<JobAlert> alerts) {
        if (alerts != null) {

            this.alerts = new ArrayList<>(alerts.size());
            this.alerts.addAll(alerts);
        } else {
            this.alerts = null;
        }
    }

    @Override
    public List<JobAlert> getAlerts() {
        return this.alerts;
    }

    @Override
    public boolean setState(JobState state) {
        return setState(state, false);
    }

    public boolean setState(JobState state, boolean force) {
        JobState curr = getState();
        if (force || (isEnabled() && curr.canTransition(state)) ||
            (!isEnabled() && state == JobState.IDLE) ||
            (!isEnabled() && state == JobState.ERROR)) {
            // Note dependence on ordering!
            this.state = state.ordinal();
            return true;
        } else if (state != curr) {
            log.warn("[job.setstate] " + ((disabled) ? "disabled " : "") + "job " +
                     getId() + " cannot transition " + curr + " -> " + state);
            for (StackTraceElement elt : Thread.currentThread().getStackTrace()) {
                log.warn(elt.toString());
            }
            return false;
        }
        return true;
    }

    public int getTaskCount() {
        return nodes.size();
    }

    @Override
    public synchronized JobTask getTask(int id) {
        if (nodes == null) {
            return null;
        }
        for (JobTask node : nodes) {
            if (node.getTaskID() == id) {
                node.setJobUUID(this.id);
                return node;
            }
        }
        return null;
    }

    @Override
    public synchronized List<JobTask> getCopyOfTasks() {
        if (nodes == null) {
            nodes = new ArrayList<>();
        }
        return ImmutableList.copyOf(nodes);
    }

    @Override
    public synchronized void addTask(JobTask task) {
        if (nodes == null) {
            nodes = new ArrayList<>();
        }
        nodes.add(task);
        if (task.getState().isActiveState()) {
            this.countActiveTasks++;
        }
    }

    private synchronized void recountActiveTasks() {
        this.countActiveTasks = 0;
        for (JobTask t : nodes) {
            if (t.getState().isActiveState()) {
                this.countActiveTasks++;
            }
        }
    }

    @Override
    public synchronized void setTasks(List<JobTask> tasks) {
        this.nodes = Lists.newArrayList(tasks);
        recountActiveTasks();
    }

    public synchronized int getCountActiveTasks() {
        return countActiveTasks;
    }

    @Override
    public JobQueryConfig getQueryConfig() {
        return queryConfig;
    }

    @Override
    public void setQueryConfig(JobQueryConfig queryConfig) {
        this.queryConfig = queryConfig;
    }

    @Override
    public JobCommand getSubmitCommand() {
        return submitCommand;
    }

    @Override
    public void setSubmitCommand(JobCommand submitCommand) {
        this.submitCommand = submitCommand;
    }

    @Override
    public boolean getStrictReplicas() {
        return strictReplicas;
    }

    @Override
    public void setStrictReplicas(boolean strictReplicas) {
        this.strictReplicas = strictReplicas;
    }

    @Override
    public HashMap<String, String> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    public JSONObject toJSON() throws Exception {
        recountActiveTasks();
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this, true);
        } catch (Exception e) {
            return super.toString();
        }
    }

    @Override
    public int compareTo(IJob o) {
        return getSubmitTime() > o.getSubmitTime() ? 1 : -1;
    }

    public synchronized boolean setTaskState(JobTask task, JobTaskState newState) {
        return setTaskState(task, newState, false);
    }

    /**
     * Change a task's state, and update the job's state if appropriate
     * @param task The task to modify
     * @param newState The new state to set
     * @param force Whether to force the state transition regardless of the expected transition map
     * @return True on success
     */
    public synchronized boolean setTaskState(JobTask task, JobTaskState newState, boolean force) {
        JobTaskState prevState = task.getState();

        if (!task.setState(newState, force)) {
            return false;
        }
        if (prevState.isActiveState() && !newState.isActiveState()) {
            this.countActiveTasks--;
        } else if (!prevState.isActiveState() && newState.isActiveState()) {
            this.countActiveTasks++;
        }
        if (newState == JobTaskState.ERROR || newState == JobTaskState.DISK_FULL) {
            this.disabled = true;
        }
        calculateJobState(force);
        return true;
    }

    /**
     * Calculate the job state based on the state of its tasks
     */
    private boolean calculateJobState(boolean force) {
        boolean err = false, sched = false, run = false, reb = false, stopped = false;
        for (JobTask t : nodes) {
            if (t.getWasStopped()) {
                stopped = true;
            }
            if (t.getState() == JobTaskState.REBALANCE) {
                reb = true;
            } else if (t.isRunning() && this.state != JobState.REBALANCE.getValue()) {
                run = true;
            } else if (t.getState() == JobTaskState.ALLOCATED || t.getState() == JobTaskState.QUEUED) {
                sched = true;
            } else if (t.getState() == JobTaskState.ERROR || t.getState() == JobTaskState.DISK_FULL) {
                err = true;
                break;
            }
        }
        JobState oldJobState = getState();
        JobState nextState = (err) ? JobState.ERROR : (reb) ? JobState.REBALANCE : (run) ? JobState.RUNNING : (sched) ? JobState.SCHEDULED : JobState.IDLE;
        if (setState(nextState, force)) {
            // If transitioning from error to non-error state, enable job as long as it has run recently.
            if (oldJobState == JobState.ERROR && nextState != JobState.ERROR && getSubmitTime() != null && System.currentTimeMillis() - getSubmitTime() < AUTO_ENABLE_CUTOFF) {
                setEnabled(true);
            }
            wasStopped = stopped;
            return true;
        } else {
            return false;
        }
    }

    public void errorTask(JobTask task, int errorCode) {
        setTaskState(task, JobTaskState.ERROR, true);
        task.setErrorCode(errorCode);
    }

    /**
     * Check whether all tasks of a job are idle/errored/rebalancing. If a job is kicked, and isFinished evaluates to true,
     * then it can be assumed that every task ran at least once, regardless of whether any rebalancing was started in the mean time.
     * @return True if the job is finished
     */
    public boolean isFinished() {
        for (JobTask jobTask : getCopyOfTasks()) {
            if (jobTask.getState() != JobTaskState.IDLE &&
                jobTask.getState() != JobTaskState.ERROR &&
                jobTask.getState() != JobTaskState.REBALANCE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getReplicationFactor() {
        return replicationFactor;
    }

    @Override
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public boolean getDontAutoBalanceMe() {
        return dontAutoBalanceMe;
    }

    @Override
    public void setDontDeleteMe(boolean dontDeleteMe) { this.dontDeleteMe = dontDeleteMe; }

    @Override
    public boolean getDontDeleteMe() {
        return dontDeleteMe;
    }

    @Override
    public void setDontAutoBalanceMe(boolean dontAutoBalanceMe) {
        this.dontAutoBalanceMe = dontAutoBalanceMe;
    }

    @Override
    public int getHourlyBackups() {
        return hourlyBackups;
    }

    @Override
    public int getDailyBackups() {
        return dailyBackups;
    }

    @Override
    public int getWeeklyBackups() {
        return weeklyBackups;
    }

    @Override
    public int getMonthlyBackups() {
        return monthlyBackups;
    }

    @Override
    public void setHourlyBackups(int hourlyBackups) {
        this.hourlyBackups = hourlyBackups;
    }

    @Override
    public void setDailyBackups(int dailyBackups) {
        this.dailyBackups = dailyBackups;
    }

    @Override
    public void setWeeklyBackups(int weeklyBackups) {
        this.weeklyBackups = weeklyBackups;
    }

    @Override
    public void setMonthlyBackups(int monthlyBackups) {
        this.monthlyBackups = monthlyBackups;
    }

    public boolean hadMoreData() {
        return hadMoreData;
    }

    public boolean getHadMoreData() {
        return hadMoreData;
    }

    public void setHadMoreData(boolean hadMore) {
        this.hadMoreData = hadMore;
    }

    public boolean wasStopped() {
        return wasStopped;
    }

    @Override
    public boolean getWasStopped() {
        return wasStopped;
    }

    @Override
    public void setWasStopped(boolean wasStopped) {
        this.wasStopped = wasStopped;
    }

    public void setTaskFinished(JobTask task) {
        int preFailErrorCode = task.getPreFailErrorCode();
        int oldErrorCode = task.getErrorCode();
        if (task.getState() == JobTaskState.REPLICATE || task.getState() == JobTaskState.BACKUP) {
            if (preFailErrorCode > 0) {
                // Restore the old job error if it existed
                errorTask(task, preFailErrorCode);
                return;
            }
        }
        task.setErrorCode(0);
        setTaskState(task, JobTaskState.IDLE);
        if (getState() == JobState.IDLE) {
            setEndTime(JitterClock.globalTime());
        }
        if (countErrorTasks() == 0 && oldErrorCode == JobTaskErrorCode.EXIT_REPLICATE_FAILURE || oldErrorCode == JobTaskErrorCode.EXIT_BACKUP_FAILURE) {
            // If the job is disabled because this task failed to replicate, enable it.
            log.warn("Enabling job " + getId() + " because the last replicate/backup error was resolved");
            disabled = false;
        }
    }

    @Override
    public int getMaxSimulRunning() {
        return maxSimulRunning;
    }

    @Override
    public void setMaxSimulRunning(int maxSimulRunning) {
        this.maxSimulRunning = maxSimulRunning;
    }

    @Override
    public int getRetries() {
        return retries;
    }

    @Override
    public void setRetries(int retries) {
        this.retries = retries;
    }

    private int countErrorTasks() {
        int count = 0;
        List<JobTask> tasks = getCopyOfTasks();
        if (tasks == null) {
            return count;
        }
        for (JobTask task : tasks) {
            if (task != null && task.getState() == JobTaskState.ERROR) {
                count++;
            }
        }
        return count;
    }

    public long calcAverageTaskSizeBytes() {
        List<JobTask> tasks = getCopyOfTasks();
        if (tasks == null || tasks.size() <= 0) {
            return 0;
        }
        long rv = 0;
        for (JobTask task : tasks) {
            if (task != null) {
                rv += task.getByteCount();
            }
        }
        return rv / (tasks.size());
    }

    @Override
    public String getMinionType() {
        if (minionType == null) {
            minionType = Minion.getDefaultMinionType();
        }
        return minionType;
    }

    public Long getCanonicalTime() {
        // Get an estimate for the last time this job was run. Use end-time if non-null; otherwise, startTime.
        return (endTime == null && getState() == JobState.IDLE) ? startTime : endTime;
    }

    @Override
    public void setMinionType(String minionType) {
        this.minionType = minionType;
    }

    public boolean shouldAutoRekick(long clock) {
        Long canonicalTime = getCanonicalTime();
        return isEnabled() && canonicalTime != null && getRunCount() > 0 && getRekickTimeout() != null &&
               getRekickTimeout() > 0 && clock - canonicalTime >= (getRekickTimeout() * 60000L);
    }
}

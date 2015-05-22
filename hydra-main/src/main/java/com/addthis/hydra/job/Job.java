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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.minion.Minion;
import com.addthis.hydra.util.LogUtil;
import com.addthis.hydra.util.StringMapHelper;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * for job submission and tracking
 * IJob that keeps everything in gone Codable Object graph
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public final class Job implements IJob {

    private static final Logger log = LoggerFactory.getLogger(Job.class);
    private static final Comparator<JobTask> taskNodeComparator =
            (t1, t2) -> Integer.compare(t1.getTaskID(), t2.getTaskID());

    @FieldConfig private int state;
    @FieldConfig private int countActiveTasks;
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
    /* minutes between re-kicking */
    @FieldConfig private Long rekickTimeout;
    /* minutes max time to allocate to job before it's interrupted */
    @FieldConfig private Long maxRunTime;
    /* list of nodes and their state */
    @FieldConfig private ArrayList<JobTask> nodes;
    /* JSON configuration url -- only read at submit time if conf empty */
    @FieldConfig private String config;
    /* URL for spawn to call on job complete. for automating workflows */
    @FieldConfig private String onComplete;
    @FieldConfig private String onError;
    /* timeout in seconds */
    @FieldConfig private int onCompleteTimeout;
    @FieldConfig private int onErrorTimeout;
    @FieldConfig private int runCount;
    @FieldConfig private long runTime;
    @FieldConfig private String command;
    @FieldConfig private boolean disabled;
    @FieldConfig private ArrayList<JobParameter> parameters;
    @FieldConfig private int hourlyBackups;
    @FieldConfig private int dailyBackups;
    @FieldConfig private int weeklyBackups;
    @FieldConfig private int monthlyBackups;
    @FieldConfig private int replicas;
    @FieldConfig private int readOnlyReplicas;
    @FieldConfig private boolean dontAutoBalanceMe;
    @FieldConfig private boolean dontDeleteMe;
    @FieldConfig private boolean dontCloneMe;
    @FieldConfig private boolean wasStopped;
    @FieldConfig private int maxSimulRunning;
    @FieldConfig private String minionType;
    @FieldConfig private boolean autoRetry;
    @FieldConfig private JobQueryConfig queryConfig;


    /* If all errored tasks from an errored job are resolved and the job has started within this cutoff, automatically
    enable the job. Default is 3 days. */
    private static final long AUTO_ENABLE_CUTOFF = Parameter.longValue("job.enable.cutoff", 1000 * 60 * 60 * 24 * 3);

    /* Task states that indicate that a job can be considered done. Rebalance/host-failure replications are included so
    these long-running operations will not delay the job rekick. */
    private static final Set<JobTaskState> taskStatesToFinishJob = ImmutableSet.of(
            JobTaskState.IDLE, JobTaskState.ERROR, JobTaskState.REBALANCE, JobTaskState.FULL_REPLICATE);

    // For codec only
    public Job() {}

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
        this.dontCloneMe = false;
        this.config = "";
        this.queryConfig = new JobQueryConfig();
    }

    public Job(IJob job) {
        this.id = job.getId();
        this.setState(job.getState());
        this.creator = job.getCreator();
        this.owner = job.getOwner();
        this.group = job.getGroup();
        this.ownerWritable = job.isOwnerWritable();
        this.groupWritable = job.isGroupWritable();
        this.worldWritable = job.isWorldWritable();
        this.ownerExecutable = job.isOwnerExecutable();
        this.groupExecutable = job.isGroupExecutable();
        this.worldExecutable = job.isWorldExecutable();
        this.lastModifiedBy = job.lastModifiedBy();
        this.lastModifiedAt = job.lastModifiedAt();
        this.description = job.getDescription();
        this.priority = job.getPriority();
        this.createTime = job.getCreateTime();
        this.submitTime = job.getSubmitTime();
        this.startTime = job.getStartTime();
        this.endTime = job.getEndTime();
        this.rekickTimeout = job.getRekickTimeout();
        this.maxRunTime = job.getMaxRunTime();
        this.setTasks(job.getCopyOfTasks());
        recountActiveTasks();
        this.config = job.getConfig();
        this.onComplete = job.getOnCompleteURL();
        this.onError = job.getOnErrorURL();
        this.onCompleteTimeout = job.getOnCompleteTimeout();
        this.onErrorTimeout = job.getOnErrorTimeout();
        this.runCount = job.getRunCount();
        this.runTime = job.getRunTime();
        this.command = job.getCommand();
        this.parameters = job.getParameters() != null ? Lists.newArrayList(job.getParameters()) : null;
        this.hourlyBackups = job.getHourlyBackups();
        this.dailyBackups = job.getDailyBackups();
        this.weeklyBackups = job.getWeeklyBackups();
        this.monthlyBackups = job.getMonthlyBackups();
        this.autoRetry = job.getAutoRetry();
        this.replicas = job.getReplicas();
        this.queryConfig = job.getQueryConfig();
        this.dontAutoBalanceMe = job.getDontAutoBalanceMe();
        this.dontDeleteMe = job.getDontDeleteMe();
        this.dontCloneMe = job.getDontCloneMe();
        this.maxSimulRunning = job.getMaxSimulRunning();
        this.minionType = job.getMinionType();
        this.wasStopped = job.getWasStopped();
        setEnabled(job.isEnabled());
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getCreator() {
        return creator;
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
    public String getGroup() {
        return group;
    }

    @Override
    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public boolean isOwnerWritable() {
        return ownerWritable;
    }

    @Override
    public void setOwnerWritable(boolean ownerWritable) {
        this.ownerWritable = ownerWritable;
    }

    @Override
    public boolean isGroupWritable() {
        return groupWritable;
    }

    @Override
    public void setGroupWritable(boolean groupWritable) {
        this.groupWritable = groupWritable;
    }

    @Override
    public boolean isWorldWritable() {
        return worldWritable;
    }

    @Override
    public void setWorldWritable(boolean worldWritable) {
        this.worldWritable = worldWritable;
    }

    @Override
    public boolean isOwnerExecutable() {
        return ownerExecutable;
    }

    @Override
    public void setOwnerExecutable(boolean ownerExecutable) {
        this.ownerExecutable = ownerExecutable;
    }

    @Override
    public boolean isGroupExecutable() {
        return groupExecutable;
    }

    @Override
    public void setGroupExecutable(boolean groupExecutable) {
        this.groupExecutable = groupExecutable;
    }

    @Override
    public boolean isWorldExecutable() {
        return worldExecutable;
    }

    @Override
    public void setWorldExecutable(boolean worldExecutable) {
        this.worldExecutable = worldExecutable;
    }

    @Override
    public String lastModifiedBy() {
        return lastModifiedBy;
    }

    @Override
    public void setLastModifiedBy(String user) {
        this.lastModifiedBy = user;
    }

    @Override
    public long lastModifiedAt() {
        return lastModifiedAt;
    }

    @Override
    public void setLastModifiedAt(long time) {
        this.lastModifiedAt = time;
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
    public int getPriority() {
        return priority;
    }

    @Override
    public void setPriority(int priority) {
        this.priority = priority;
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
                    if (state == JobTaskState.ERROR) {
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
    public String getOnCompleteURL() { return onComplete; }

    @Override
    public void setOnCompleteURL(String url) { this.onComplete = url; }

    @Override
    public String getOnErrorURL() { return onError; }

    @Override
    public void setOnErrorURL(String url) { this.onError = url; }

    @Override
    public int getOnCompleteTimeout() { return onCompleteTimeout; }

    @Override
    public void setOnCompleteTimeout(int timeout) { this.onCompleteTimeout = timeout; }

    @Override
    public int getOnErrorTimeout() { return onErrorTimeout; }

    @Override
    public void setOnErrorTimeout(int timeout) { this.onErrorTimeout = timeout; }

    @Override
    public int getReplicas() {
        return replicas;
    }

    @Override
    public void setReplicas(int replicas) {
        this.replicas = replicas;
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
    public boolean setState(JobState state) {
        return setState(state, false);
    }

    public boolean setState(JobState state, boolean force) {
        JobState curr = getState();
        if (force
            || (isEnabled() && curr.canTransition(state))
            || (!isEnabled() && (state == JobState.IDLE))
            || (!isEnabled() && (state == JobState.ERROR))) {
            // Note dependence on ordering!
            this.state = state.ordinal();
            return true;
        } else if (state != curr) {
            log.warn("[job.setstate] {}job {} cannot transition {} -> {}",
                     (disabled) ? "disabled " : "", getId(), curr, state);
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

    public List<JobTask> getCopyOfTasksSorted() {
        if (nodes == null) {
            nodes = new ArrayList<>();
        }
        List<JobTask> tasksCopy = Lists.newArrayList(nodes);
        Collections.sort(tasksCopy, taskNodeComparator);
        return tasksCopy;
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

    @Override public JSONObject toJSON() throws Exception {
        recountActiveTasks();
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this);
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
        if (newState == JobTaskState.ERROR) {
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
            } else if (t.isRunning()) {
                run = true;
            } else if (t.getState() == JobTaskState.ALLOCATED || t.getState().isQueuedState()) {
                sched = true;
            } else if (t.getState() == JobTaskState.ERROR) {
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
            if (!taskStatesToFinishJob.contains(jobTask.getState())) {
                return false;
            }
        }
        return true;
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
    public void setDontCloneMe(boolean dontCloneMe) { this.dontCloneMe = dontCloneMe; }

    @Override
    public boolean getDontCloneMe() {
        return dontCloneMe;
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
        setTaskState(task, JobTaskState.IDLE, true);
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
    public boolean getAutoRetry() {
        return autoRetry;
    }

    @Override
    public void setAutoRetry(boolean autoRetry) {
        this.autoRetry = autoRetry;
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
            minionType = Minion.defaultMinionType;
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

    /**
     * Log a job event to a rolling log file
     */
    public static void logJobEvent(Job job, JobEvent event, RollingLog eventLog) {
        LogUtil.log(eventLog, log, new StringMapHelper()
                        .put("event", event)
                        .put("time", System.currentTimeMillis())
                        .put("jobid", job.getId())
                        .put("creator", job.getCreator())
                        .put("owner", job.getOwner())
                        .put("createTime", job.getCreateTime())
                        .put("priority", job.getPriority())
                        .put("replicas", job.getReplicas())
                        .put("runCount", job.getRunCount())
                        .put("state", job.getState())
                        .put("taskCount", job.getTaskCount())
                        .put("avgTaskSize", job.calcAverageTaskSizeBytes())
                        .put("startTime", job.getStartTime())
                        .put("endTime", job.getEndTime())
                        .put("submitTime", job.getSubmitTime())
                        .put("command", job.getCommand()));
    }
}

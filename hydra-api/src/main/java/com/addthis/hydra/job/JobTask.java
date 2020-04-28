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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.maljson.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * smallest unit of a job assigned to a host
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties({"readOnlyReplicas", "replicationFactor"})
public final class JobTask implements Codable, Cloneable, Comparable<JobTask> {

    private static final Logger log = LoggerFactory.getLogger(JobTask.class);

    @FieldConfig private String hostUuid;
    @FieldConfig private String jobUuid;
    @FieldConfig private int node;
    @FieldConfig private int state;
    @FieldConfig private int runCount;
    @FieldConfig private int starts;
    @FieldConfig private long startTime;
    @FieldConfig private int errors;
    @FieldConfig private long fileCount;
    @FieldConfig private long fileBytes;
    @FieldConfig private int port;
    @FieldConfig private int errorCode;
    @FieldConfig private boolean wasStopped;
    @FieldConfig private int preFailErrorCode;
    @FieldConfig private long input;
    @FieldConfig private double meanRate;
    @FieldConfig private long totalEmitted;
    @FieldConfig private String rebalanceSource;
    @FieldConfig private String rebalanceTarget;
    @FieldConfig private ArrayList<JobTaskReplica> replicas;

    private volatile JobKey jobKey;

    public JobTask() {}

    // Only used for Testing right now
    @VisibleForTesting
    public JobTask(String hostUuid, int node, int runCount) {
        this.hostUuid = hostUuid;
        this.jobUuid = "";
        this.node = node;
        this.runCount = runCount;
        jobKey = new JobKey(jobUuid, node);
    }

    private static final Set<JobTaskState> nonRunningStates = ImmutableSet.of(JobTaskState.IDLE,
                                                                              JobTaskState.ERROR,
                                                                              JobTaskState.ALLOCATED,
                                                                              JobTaskState.REBALANCE,
                                                                              JobTaskState.QUEUED_HOST_UNAVAIL,
                                                                              JobTaskState.QUEUED_NO_SLOT,
                                                                              JobTaskState.QUEUED);

    public boolean isRunning() {
        JobTaskState taskState = getState();
        return !nonRunningStates.contains(taskState);
    }

    public void setHostUUID(String uuid) {
        hostUuid = uuid;
    }

    public String getHostUUID() {
        return hostUuid;
    }

    public void setJobUUID(String uuid) {
        getJobKey().setJobUuid(uuid);
        jobUuid = uuid;
    }

    public String getJobUUID() {
        return jobUuid;
    }

    public void setTaskID(int id) {
        getJobKey().setNodeNumber(id);
        node = id;
    }


    public int getTaskID() {
        return node;
    }

    public JobTaskState getState() {
        JobTaskState taskState = JobTaskState.makeState(state);
        return taskState == null ? JobTaskState.UNKNOWN : taskState;
    }

    public boolean setState(JobTaskState state) {
        return setState(state, false);
    }

    public boolean setState(JobTaskState state, boolean force) {
        JobTaskState curr = getState();
        if (force || curr.canTransition(state)) {
            this.state = state.ordinal();
            return true;
        } else if (state != curr) {
            log.warn("[task.setstate] task {} cannot transition {} -> {}",
                     getTaskID(), curr, state, new Exception("Stack Trace"));
            return false;
        }
        return true;
    }

    public int getRunCount() {
        return runCount;
    }

    public void setRunCount(int runCount) {
        this.runCount = runCount;
    }

    public int getStarts() {
        return starts;
    }

    public int incrementStarts() {
        return ++starts;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long time) {
        this.startTime = time;
    }

    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    public int incrementErrors() {
        return ++errors;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public long getByteCount() {
        return fileBytes;
    }

    public void setByteCount(long byteCount) {
        this.fileBytes = byteCount;
    }

    // todo: Can we change the contract so it does not rely on this being mutated?
    public List<JobTaskReplica> getReplicas() {
        return replicas;
    }

    public boolean hasReplicaOnHost(String hostUuid) {
        for (JobTaskReplica replica : getAllReplicas()) {
            if (replica != null && replica.getHostUUID().equals(hostUuid)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasReplicaOnHosts(Collection<String> hostUuids) {
        for (String hostUuid : hostUuids) {
            if (hasReplicaOnHost(hostUuid)) {
                return true;
            }
        }
        return false;
    }

    public void setReplicas(List<JobTaskReplica> replicas) {
        this.replicas = Lists.newArrayList(replicas);
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        return "[JobNode:" + hostUuid + "/" + node + "#" + runCount + "]";
    }

    @Override
    public int compareTo(JobTask o) {
        return Integer.valueOf(getTaskID()).compareTo(o.getTaskID());
    }

    public synchronized JobKey getJobKey() {
        if (jobKey == null) {
            jobKey = new JobKey(jobUuid, node);
        }
        return jobKey;
    }

    public List<JobTaskReplica> getAllReplicas() {
        List<JobTaskReplica> replicaList = new ArrayList<>();
        if (replicas != null) {
            replicaList.addAll(replicas);
        }
        return replicaList;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getPreFailErrorCode() {
        return preFailErrorCode;
    }

    public void setPreFailErrorCode(int code) {
        preFailErrorCode = code;
    }

    public void setWasStopped(boolean wasStopped) {
        this.wasStopped = wasStopped;
    }

    public boolean getWasStopped() {
        return wasStopped;
    }

    public long getInput() {
        return input;
    }

    public void setInput(long input) {
        this.input = input;
    }

    public double getMeanRate() {
        return meanRate;
    }

    public void setMeanRate(double meanRate) {
        this.meanRate = meanRate;
    }

    public long getTotalEmitted() {
        return totalEmitted;
    }

    public void setTotalEmitted(long totalEmitted) {
        this.totalEmitted = totalEmitted;
    }

    public String getRebalanceTarget() {
        return rebalanceTarget;
    }

    public void setRebalanceTarget(@Nullable String rebalanceTarget) {
        this.rebalanceTarget = rebalanceTarget;
    }

    public String getRebalanceSource() {
        return rebalanceSource;
    }

    public void setRebalanceSource(@Nullable String rebalanceSource) {
        this.rebalanceSource = rebalanceSource;
    }

    public List<String> getAllTaskHosts() {
        List<String> rv = new ArrayList<>();
        rv.add(hostUuid);
        if (getAllReplicas() != null) {
            for (JobTaskReplica replica : getAllReplicas()) {
                if (replica != null && replica.getHostUUID() != null) {
                    rv.add(replica.getHostUUID());
                }
            }
        }
        return rv;
    }

    /**
     * resets a task's tracking metrics.  Used in cases where an existing
     * task has had its data scrubbed and is essentially starting fresh
     */
    public void resetTaskMetrics() {
        starts = 0;
        setByteCount(0);
        setFileCount(0);
        setErrorCode(0);
        setRunCount(0);
        setInput(0);
        setMeanRate(0);
        setTotalEmitted(0);
    }

    public void replaceReplica(String failedHostUuid, String newHostUuid) {
        if (getAllReplicas() != null) {
            for (JobTaskReplica replica : getAllReplicas()) {
                if (failedHostUuid.equals(replica.getHostUUID())) {
                    replica.setHostUUID(newHostUuid);
                }
            }
        }
    }
}

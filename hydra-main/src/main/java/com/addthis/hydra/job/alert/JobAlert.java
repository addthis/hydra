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
package com.addthis.hydra.job.alert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bean to hold a job specific alert
 */
public class JobAlert implements Codable {

    private static final Logger log = LoggerFactory.getLogger(JobAlert.class);

    //Alert types
    public static final int ON_ERROR = 0;
    public static final int ON_COMPLETE = 1;
    public static final int RUNTIME_EXCEEDED = 2;
    public static final int REKICK_TIMEOUT = 3;
    public static final int SPLIT_CANARY = 4;
    public static final int MAP_CANARY = 5;
    public static final int MAP_FILTER_CANARY = 6;

    @FieldConfig(codable = true)
    private String alertId;
    @FieldConfig(codable = true)
    private long lastAlertTime;
    @FieldConfig(codable = true)
    private int type;
    @FieldConfig(codable = true)
    private int timeout;
    @FieldConfig(codable = true)
    private String email;
    @FieldConfig(codable = true)
    private String[] jobIds;
    @FieldConfig(codable = true)
    private String canaryPath;
    @FieldConfig(codable = true)
    private String canaryRops;
    @FieldConfig(codable = true)
    private String canaryOps;
    @FieldConfig(codable = true)
    private String canaryFilter;
    @FieldConfig(codable = true)
    private int canaryConfigThreshold;
    @FieldConfig(codable = true)
    private String description;

    private String canaryOutputMessage;

    /* For alerts tracking multiple jobs, this variable marks if the set of active jobs has changed since the last alert check */
    private boolean hasChanged = false;

    /* Number of milliseconds in one minute */
    private static final int MINUTE = 60 * 1000;

    /* Map storing {job id : job description} for all alerted jobs the last time this alert was checked */
    @FieldConfig(codable = true)
    private final HashMap<String, String> activeJobs;

    /* Map temporarily storing prior active jobs that have since cleared */
    private final HashMap<String, String> priorActiveJobs;

    private Long lastActual = 0l;

    private static final ImmutableMap<Integer, String> alertMessageMap = new ImmutableMap.Builder<Integer,String>().put(ON_ERROR, "Task is in Error ")
            .put(ON_COMPLETE, "Task has Completed ")
            .put(RUNTIME_EXCEEDED, "Task runtime exceeded ")
            .put(REKICK_TIMEOUT, "Task rekick exceeded ")
            .put(SPLIT_CANARY, "Split canary ")
            .put(MAP_CANARY, "Map canary ")
            .put(MAP_FILTER_CANARY, "Bundle canary ")
            .build();

    public JobAlert() {
        this.lastAlertTime = -1;
        this.type = 0;
        this.timeout = 0;
        this.email = "";
        this.description = "";
        activeJobs = new HashMap<>();
        priorActiveJobs = new HashMap<>();
    }

    public JobAlert(String alertId, int type, int timeout, String email, String description, String[] jobIds) {
        this.alertId = alertId;
        this.lastAlertTime = -1;
        this.type = type;
        this.timeout = timeout;
        this.email = email;
        this.description = description;
        this.jobIds = jobIds;
        activeJobs = new HashMap<>();
        priorActiveJobs = new HashMap<>();
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public boolean hasAlerted() {
        return this.lastAlertTime > 0;
    }

    public void alerted() {
        this.lastAlertTime = JitterClock.globalTime();
    }

    public void clear() {
        synchronized (activeJobs) {
            activeJobs.clear();
        }
        this.lastAlertTime = -1;
    }

    public void setActiveJobs(Map<String, String> activeJobsNew) {
        synchronized (activeJobs) {
            activeJobs.clear();
            activeJobs.putAll(activeJobsNew);
        }
    }

    public long getLastAlertTime() {
        return lastAlertTime;
    }

    public void setLastAlertTime(long lastAlertTime) {
        this.lastAlertTime = lastAlertTime;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getEmail() {return email; }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getDescription() {return description; }

    public void setDescription(String description) {
        this.description = description;
    }

    public String[] getJobIds() {
        return jobIds;
    }

    public void setJobIds(String[] jobIds) {
        this.jobIds = jobIds;
    }

    public String getCanaryPath() {
        return canaryPath;
    }

    public void setCanaryPath(String canaryPath) { this.canaryPath = canaryPath; }

    public String getCanaryRops() { return canaryRops; }

    public void setCanaryRops(String canaryRops) {
        this.canaryRops = canaryRops;
    }

    public String getCanaryOps() { return canaryOps; }

    public void setCanaryOps(String canaryOps) {
        this.canaryOps = canaryOps;
    }

    public String getCanaryFilter() { return canaryFilter; }

    public void setCanaryFilter(String canaryFilter) {
        this.canaryFilter = canaryFilter;
    }

    public Integer getCanaryConfigThreshold() {
        return canaryConfigThreshold;
    }

    public void setCanaryConfigThreshold(Integer canaryConfigThreshold) {
        this.canaryConfigThreshold = canaryConfigThreshold;
    }

    public String getCanaryOutputMessage() {
        return canaryOutputMessage;
    }

    public void appendCanaryOutputMessage(String canaryOutputMessage) {
        this.canaryOutputMessage += canaryOutputMessage;
    }



    public JSONObject toJSON() throws Exception {
        JSONObject rv = CodecJSON.encodeJSON(this);
        if (jobIds != null) {
            rv.put("jobIds", Strings.join(jobIds, ","));
        }
        return rv;
    }

    /**
     * Check this alert's jobs to see if any are active.
     * @param jobs A list of jobs to check
     * @return True if the alert has changed state from fired to cleared or vice versa
     */
    public boolean checkAlertForJobs(List<Job> jobs, MeshyClient meshyClient) {
        boolean activeNow = false;
        HashMap<String, String> activeJobBefore;
        HashMap<String, String> activeJobAfter;
        synchronized (activeJobs) {
            activeJobBefore = new HashMap<>(activeJobs);
            activeJobs.clear();
            for (Job job : jobs) {
                if (alertActiveForJob(meshyClient, job)) {
                    activeNow = true;
                    activeJobs.put(job.getId(), job.getDescription());
                    // Don't break the loop to ensure that all triggering jobs will be added to activeJobs
                }
            }
            activeJobAfter = new HashMap<>(activeJobs);
        }
        if (activeNow && !hasAlerted()) {
            alerted();
            return true;
        } else if (!activeNow && hasAlerted()) {
            priorActiveJobs.clear();
            priorActiveJobs.putAll(activeJobBefore);
            clear();
            return true;
        } else if (!activeJobBefore.equals(activeJobAfter)) {
            hasChanged = true;
            return true;
        }
        return false;
    }

    public Map<String, String> getActiveJobs() {
        synchronized (activeJobs) {
            return ImmutableMap.copyOf(activeJobs);
        }
    }

    @JsonIgnore
    public String getAlertStatus() {
        hasChanged = false;
        StringBuilder sb = new StringBuilder();
        sb.append( hasAlerted() ? (hasChanged ? "[CHANGE] " : "[TRIGGER] ") : "[CLEAR] " );
        sb.append( alertMessageMap.containsKey(type) ? alertMessageMap.get(type) : "unknown alert" );
        sb.append(" - ");
        sb.append(JobAlertRunner.getClusterHead());
        sb.append(" - ");
        sb.append(hasAlerted() ? getActiveJobs().toString() : priorActiveJobs.toString());
        if (hasLastActual()) {
            sb.append(" - expected=" + canaryConfigThreshold + " actual=" + lastActual);
        }
        return sb.toString();
    }

    private boolean alertActiveForJob(MeshyClient meshClient, Job job) {
        long currentTime = System.currentTimeMillis();
        switch (type) {
            case ON_ERROR:
                return job.getState().equals(JobState.ERROR);
            case ON_COMPLETE:
                return job.getState().equals(JobState.IDLE);
            case RUNTIME_EXCEEDED:
                return (job.getState().equals(JobState.RUNNING) && (job.getStartTime() != null) &&
                    ((currentTime - job.getStartTime()) > timeout * MINUTE));
            case REKICK_TIMEOUT:
                return (!job.getState().equals(JobState.RUNNING) && (job.getEndTime() != null) &&
                    ((currentTime - job.getEndTime()) > timeout * MINUTE));
            case SPLIT_CANARY:
                return checkSplitCanary(meshClient, job);
            case MAP_CANARY:
                return checkMapCanary(job);
            case MAP_FILTER_CANARY:
                return checkMapFilterCanary(job);
            default:
                log.warn("Warning: alert " + alertId + " has unexpected type " + type);
                return false;
        }
    }

    private boolean checkMapCanary(Job job) {
        if (isValid() != null) {
            return false;
        }
        try {
            long queryVal = JobAlertUtil.getQueryCount(job.getId(), canaryPath);
            lastActual = queryVal;
            return queryVal < canaryConfigThreshold;
        } catch (Exception ex) {
            log.warn("Exception during canary check: ", ex);
            return false;
        }
    }

    private boolean checkMapFilterCanary(Job job) {
        if (isValid() != null) {
            return false;
        }
        try {
            canaryOutputMessage = "";
            boolean[] result = JobAlertUtil.evaluateQueryWithFilter(this, job.getId());
            boolean failure = false;
            for(int i = 0; i < result.length; i++) {
                failure = failure || !result[i];
            }
            return failure;
        } catch (Exception ex) {
            log.warn("Exception during canary check: ", ex);
            canaryOutputMessage += ex.toString() + "\n";
            return false;
        }
    }

    private boolean checkSplitCanary(MeshyClient meshClient, Job job) {
        if (isValid() != null) {
            return false;
        }
        // Strip off preceding slash, if it exists.
        String finalPath = canaryPath.startsWith("/") ? canaryPath.substring(1) : canaryPath;
        long totalBytes = JobAlertUtil.getTotalBytesFromMesh(meshClient, job.getId(), finalPath);
        lastActual = totalBytes;
        return totalBytes < canaryConfigThreshold;
    }

    /**
     * Returns either a message indicating an error with the configuration
     * or null if the configuration is valid.
     *
     * @return null if configuration is valid.
     */
    public String isValid() {
        switch (type) {
            case ON_ERROR:
            case ON_COMPLETE:
            case RUNTIME_EXCEEDED:
            case REKICK_TIMEOUT:
                return null;
            case SPLIT_CANARY:
            case MAP_CANARY:
                if (Strings.isEmpty(canaryPath)) {
                    return "Canary path is empty";
                } else if (canaryConfigThreshold <= 0) {
                    return "Canary config is not a positive integer";
                } else {
                    return null;
                }
            case MAP_FILTER_CANARY:
                if (Strings.isEmpty(canaryPath)) {
                    return "Canary path is empty";
                } else if (Strings.isEmpty(canaryFilter)) {
                    return "Canary filter is empty";
                }
                try {
                    CodecJSON.decodeString(BundleFilter.class, canaryFilter);
                } catch (Exception ex) {
                    return "Error attempting to create bundle filter";
                }
                return null;
            default:
                return "alert " + alertId + " has unexpected type " + type;
        }
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this);
        } catch (Exception e) {
            return super.toString();
        }
    }

    public boolean hasLastActual() {
        return type == SPLIT_CANARY || type == MAP_CANARY;
    }
}

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
package com.addthis.hydra.job.spawn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.JitterClock;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bean to hold a job specific alert
 */
public class JobAlert implements Codec.Codable {

    private static final Logger log = LoggerFactory.getLogger(JobAlert.class);

    //Alert types
    private static final int ON_ERROR = 0;
    private static final int ON_COMPLETE = 1;
    private static final int RUNTIME_EXCEEDED = 2;
    private static final int REKICK_TIMEOUT = 3;

    @Codec.Set(codable = true)
    private String alertId;
    @Codec.Set(codable = true)
    private long lastAlertTime;
    @Codec.Set(codable = true)
    private int type;
    @Codec.Set(codable = true)
    private int timeout;
    @Codec.Set(codable = true)
    private String email;
    @Codec.Set(codable = true)
    private String[] jobIds;

    private static final int MINUTE = 60 * 1000;

    /* Map storing {job id : job description} for all alerted jobs the last time this alert was checked */
    private final HashMap<String, String> activeJobs;

    private static final Map<Integer, String> alertMessageMap = ImmutableMap.of(ON_ERROR, "Task is in Error ",
            ON_COMPLETE, "Task has Completed ",
            RUNTIME_EXCEEDED, "Task runtime exceeded ",
            REKICK_TIMEOUT, "Task rekick exceeded ");

    public JobAlert() {
        this.lastAlertTime = -1;
        this.type = 0;
        this.timeout = 0;
        this.email = "";
        activeJobs = new HashMap<>();
    }

    public JobAlert(String alertId, int type, int timeout, String email, String[] jobIds) {
        this.alertId = alertId;
        this.lastAlertTime = -1;
        this.type = type;
        this.timeout = timeout;
        this.email = email;
        this.jobIds = jobIds;
        activeJobs = new HashMap<>();
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
        this.lastAlertTime = -1;
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

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String[] getJobIds() {
        return jobIds;
    }

    public void setJobIds(String[] jobIds) {
        this.jobIds = jobIds;
    }

    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    /**
     * Check this alert's jobs to see if any are active.
     * @param jobs A list of jobs to check
     * @return True if the alert has changed state from fired to cleared or vice versa
     */
    public boolean checkAlertForJobs(List<Job> jobs) {
        boolean activeNow = false;
        synchronized (activeJobs) {
            activeJobs.clear();
            for (Job job : jobs) {
                if (alertActiveForJob(job)) {
                    activeNow = true;
                    activeJobs.put(job.getId(), job.getDescription());
                    // Don't break the loop to ensure that all triggering jobs will be added to activeJobs
                }
            }
        }
        if (activeNow && !hasAlerted()) {
            alerted();
            return true;
        } else if (!activeNow && hasAlerted()) {
            clear();
            return true;
        }
        return false;
    }

    public Map<String, String> getActiveJobs() {
        synchronized (activeJobs) {
            return ImmutableMap.copyOf(activeJobs);
        }
    }

    public String getCurrentStateMessage() {
        String base = hasAlerted() ? "[TRIGGER] " : "[CLEAR] ";
        if (alertMessageMap.containsKey(type)) {
            return base + alertMessageMap.get(type);
        }
        return base + " unknown alert";
    }

    private boolean alertActiveForJob(Job job) {
        long currentTime = System.currentTimeMillis();
        switch (type) {
            case ON_ERROR:
                return job.getState().equals(JobState.ERROR);
            case ON_COMPLETE:
                // job is idle and has completed within the last 60 minutes
                return job.getState().equals(JobState.IDLE);
            case RUNTIME_EXCEEDED:
                return (job.getState().equals(JobState.RUNNING) && (job.getSubmitTime() != null) &&
                    ((currentTime - job.getSubmitTime()) > timeout * MINUTE));
            case REKICK_TIMEOUT:
                return (!job.getState().equals(JobState.RUNNING) && (job.getEndTime() != null) &&
                    ((currentTime - job.getEndTime()) > timeout * MINUTE));
            default:
                log.warn("Warning: alert " + alertId + " has unexpected type " + type);
                return false;
        }
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this, true);
        } catch (Exception e) {
            return super.toString();
        }
    }
}

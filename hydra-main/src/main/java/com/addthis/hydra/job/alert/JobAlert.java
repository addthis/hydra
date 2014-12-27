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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.SocketTimeoutException;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.Time;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bean to hold a job specific alert
 */
@JsonIgnoreProperties({"alertStatus", "canaryOutputMessage"})
public class JobAlert implements Codable {
    private static final Logger log = LoggerFactory.getLogger(JobAlert.class);

    private static final long MILLIS_PER_MINUTE = 1000l * 60l;

    /** Trigger alert if number of consecutive canary check exception is >= this limit */
    private static final int MAX_CONSECUTIVE_CANARY_EXCEPTION = 3;

    //Alert types
    public static final int ON_ERROR = 0;
    public static final int ON_COMPLETE = 1;
    public static final int RUNTIME_EXCEEDED = 2;
    public static final int REKICK_TIMEOUT = 3;
    public static final int SPLIT_CANARY = 4;
    public static final int MAP_CANARY = 5;
    public static final int MAP_FILTER_CANARY = 6;

    private static final ImmutableMap<Integer, String> alertMessageMap =
            new ImmutableMap.Builder<Integer,String>().put(ON_ERROR, "Task is in Error ")
                                                      .put(ON_COMPLETE, "Task has Completed ")
                                                      .put(RUNTIME_EXCEEDED, "Task runtime exceeded ")
                                                      .put(REKICK_TIMEOUT, "Task rekick exceeded ")
                                                      .put(SPLIT_CANARY, "Split canary ")
                                                      .put(MAP_CANARY, "Map canary ")
                                                      .put(MAP_FILTER_CANARY, "Bundle canary ")
                                                      .build();

    @Nonnull @JsonProperty public final String alertId;
    @JsonProperty public final String description;
    @JsonProperty public final int type;
    @JsonProperty public final int timeout;
    @JsonProperty public final int delay;
    @JsonProperty public final String email;
    @JsonProperty public final ImmutableList<String> jobIds;
    @JsonProperty public final String canaryPath;
    @JsonProperty public final String canaryOps;
    @JsonProperty public final String canaryRops;
    @JsonProperty public final String canaryFilter;
    @JsonProperty public final int canaryConfigThreshold;

    /* Map storing {job id : error description} for all alerts that require state change */
    @JsonProperty private volatile ImmutableMap<String, JobAlertUpdate> activeAlerts;

    // does not distinguish between multiple jobs, and racey wrt activeJobs, but only used for web-ui code for humans
    @JsonProperty private volatile long lastAlertTime;

    /** Running count of consecutive canary query exceptions. Reset on success. */
    private final transient AtomicInteger consecutiveCanaryExceptionCount = new AtomicInteger(0);

    @JsonCreator
    public JobAlert(@Nullable @JsonProperty("alertId") String alertId,
                    @JsonProperty("description") String description,
                    @JsonProperty(value = "type", required = true) int type,
                    @Time(TimeUnit.MINUTES) @JsonProperty("timeout") int timeout,
                    @Time(TimeUnit.MINUTES) @JsonProperty("delay") int delay,
                    @JsonProperty("email") String email,
                    @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                    @JsonProperty("canaryPath") String canaryPath,
                    @JsonProperty("canaryOps") String canaryOps,
                    @JsonProperty("canaryRops") String canaryRops,
                    @JsonProperty("canaryFilter") String canaryFilter,
                    @JsonProperty("canaryConfigThreshold") int canaryConfigThreshold,
                    @JsonProperty("lastAlertTime") long lastAlertTime,
                    @JsonProperty("activeJobs") Map<String, String> activeJobs,
                    @JsonProperty("activeAlerts") Map<String, JobAlertUpdate> activeAlerts) {
        if (alertId == null) {
            String newAlertId = UUID.randomUUID().toString();
            log.debug("creating new alert with uuid: {}", newAlertId);
            this.alertId = newAlertId;
        } else {
            this.alertId = alertId;
        }
        this.description = description;
        this.type = type;
        this.timeout = timeout;
        this.delay = delay;
        this.email = email;
        this.jobIds = ImmutableList.copyOf(jobIds);
        this.canaryPath = canaryPath;
        this.canaryOps = canaryOps;
        this.canaryRops = canaryRops;
        this.canaryFilter = canaryFilter;
        this.canaryConfigThreshold = canaryConfigThreshold;
        if (activeJobs != null) {
            this.activeAlerts = generateActiveAlerts(activeJobs);
        } else {
            this.activeAlerts = ImmutableMap.copyOf(activeAlerts);
        }
        this.lastAlertTime = lastAlertTime;
    }

    private static ImmutableMap<String, JobAlertUpdate> generateActiveAlerts(Map<String,String> activeJobs) {
        ImmutableMap.Builder<String, JobAlertUpdate> builder = new ImmutableMap.Builder<>();
        for(Map.Entry<String,String> entry : activeJobs.entrySet()) {
            builder.put(entry.getKey(), new JobAlertUpdate(entry.getValue(), 0l, JobAlertState.TRIGGER_SENT_EMAIL));
        }
        return builder.build();
    }

    // getters/setters that trigger ser/deser and are not vanilla (also have in-code usages)

    public Map<String, JobAlertUpdate> getActiveAlerts() {
        return activeAlerts;
    }

    public void setActiveAlerts(Map<String, JobAlertUpdate> activeAlerts) {
        this.activeAlerts = ImmutableMap.copyOf(activeAlerts);
    }

    // used by the ui/ web code
    @Deprecated public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    @VisibleForTesting
    public static JobAlertUpdate generateNext(JobAlertUpdate previous, String message, long now, int delay) {
        boolean sendNow = (delay <= 0);
        // case 1: no alert at current iteration
        if (message == null) {
            if (previous == null ||
                previous.state == JobAlertState.CLEAR_SENDING_EMAIL ||
                previous.state == JobAlertState.TRIGGER_DELAY_EMAIL) {
                return null;
            } else {
                return new JobAlertUpdate(previous.error, 0, JobAlertState.CLEAR_SENDING_EMAIL);
            }
        // case 2: no alert at previous iteration
        } else if (previous == null) {
            return new JobAlertUpdate(message, now, sendNow ?
                JobAlertState.TRIGGER_SENDING_EMAIL :
                JobAlertState.TRIGGER_DELAY_EMAIL);
        // case 3: alerts at current and previous iteration
        } else {
            switch (previous.state) {
                case CLEAR_SENDING_EMAIL:
                    if (sendNow) {
                        return new JobAlertUpdate(message, now, JobAlertState.TRIGGER_SENDING_EMAIL);
                    } else {
                        return new JobAlertUpdate(message, now, JobAlertState.TRIGGER_DELAY_EMAIL);
                    }
                case TRIGGER_DELAY_EMAIL:
                    if (previous.timestamp + delay * MILLIS_PER_MINUTE < now) {
                        return new JobAlertUpdate(message, previous.timestamp, JobAlertState.TRIGGER_SENDING_EMAIL);
                    } else {
                        return new JobAlertUpdate(message, previous.timestamp, JobAlertState.TRIGGER_DELAY_EMAIL);
                    }
                case TRIGGER_SENDING_EMAIL:
                case TRIGGER_SENDING_CHANGED:
                case TRIGGER_SENT_EMAIL:
                    if (previous.error.equals(message)) {
                        return new JobAlertUpdate(message, previous.timestamp, JobAlertState.TRIGGER_SENT_EMAIL);
                    } else {
                        return new JobAlertUpdate(message, previous.timestamp, JobAlertState.TRIGGER_SENDING_CHANGED);
                    }
                default:
                    throw new IllegalStateException("Unknown state: " + previous.state);
            }
        }
    }

    public void checkAlertForJobs(Set<Job> jobs, MeshyClient meshyClient) {
        long now = System.currentTimeMillis();
        ImmutableMap.Builder<String, JobAlertUpdate> builder = new ImmutableMap.Builder<>();
        for (Job job : jobs) {
            JobAlertUpdate previous = activeAlerts.get(job.getId());
            String previousError = (previous == null) ? null : previous.error;
            String errorMessage = alertActiveForJob(meshyClient, job, previousError);
            JobAlertUpdate next = generateNext(previous, errorMessage, now, delay);
            if (next != null) {
                builder.put(job.getId(), next);
            }
        }
        activeAlerts = builder.build();
        if (activeAlerts.isEmpty()) {
            lastAlertTime = 0;
        } else if (lastAlertTime <= 0) {
            lastAlertTime = now;
        }
    }

    @JsonIgnore
    public String getTypeString() {
        if (alertMessageMap.containsKey(type)) {
            return alertMessageMap.get(type);
        } else {
            return "unknown alert";
        }
    }

    @VisibleForTesting
    @Nullable
    String alertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        String validationError = isValid();
        if (validationError != null) {
            return validationError;
        }
        long currentTime = System.currentTimeMillis();
        switch (type) {
            case ON_ERROR:
                if (job.getState() == JobState.ERROR) {
                    return job.getCopyOfTasksSorted().stream()
                              .map(task -> "Task " + task.getTaskID() + " -> " + task.getErrorCode())
                              .collect(Collectors.joining("\n"));
                }
                break;
            case ON_COMPLETE:
                if (job.getState() == JobState.IDLE) {
                    return job.getState().name();
                }
                break;
            case RUNTIME_EXCEEDED:
                if ((job.getState() == JobState.RUNNING) && (job.getStartTime() != null)) {
                    long runningTime = currentTime - job.getStartTime();
                    if (runningTime > TimeUnit.MINUTES.toMillis(timeout)) {
                        return "Job startTime is " + new Date(job.getStartTime());
                    }
                }
                break;
            case REKICK_TIMEOUT:
                if ((job.getState() != JobState.RUNNING) && (job.getEndTime() != null)) {
                    long rekickTime = currentTime - job.getEndTime();
                    if (rekickTime > TimeUnit.MINUTES.toMillis(timeout)) {
                        return "Job endTime is " + new Date(job.getEndTime());
                    }
                }
                break;
            case SPLIT_CANARY:
                return checkSplitCanary(meshClient, job);
            case MAP_CANARY:
                return checkMapCanary(job, previousErrorMessage);
            case MAP_FILTER_CANARY:
                return checkMapFilterCanary(job, previousErrorMessage);
            default:
                log.warn("Warning: alert {} has unexpected type {}", alertId, type);
                return "unexpected alert type: " + type;
        }
        return null;
    }

    @Nullable private String checkMapCanary(Job job, String previousErrorMessage) {
        try {
            long queryVal = JobAlertUtil.getQueryCount(job.getId(), canaryPath);
            consecutiveCanaryExceptionCount.set(0);
            if (queryVal < canaryConfigThreshold) {
                return "query value: " + queryVal + " < " + canaryConfigThreshold;
            }
        } catch (Exception ex) {
            return handleCanaryException(ex, previousErrorMessage);
        }
        return null;
    }

    @Nullable private String checkMapFilterCanary(Job job, String previousErrorMessage) {
        try {
            String s = JobAlertUtil.evaluateQueryWithFilter(this, job.getId());
            consecutiveCanaryExceptionCount.set(0);
            return s;
        } catch (Exception ex) {
            return handleCanaryException(ex, previousErrorMessage);
        }
    }

    @VisibleForTesting
    @Nullable String handleCanaryException(Exception ex, @Nullable String previousErrorMessage) {
        log.warn("Exception during canary check: ", ex);
        // special handling for SocketTimeoutException which is mostly trasient
        if (Throwables.getRootCause(ex) instanceof SocketTimeoutException) {
            int c = consecutiveCanaryExceptionCount.incrementAndGet();
            if (c >= MAX_CONSECUTIVE_CANARY_EXCEPTION) {
                consecutiveCanaryExceptionCount.set(0);
                return "Canary check threw exception at least " + MAX_CONSECUTIVE_CANARY_EXCEPTION + " times in a row. " +
                       "The most recent error is: " + ex.getMessage();
            } else {
                return previousErrorMessage;
            }
        }
        return ex.getMessage();
    }

    @Nullable private String checkSplitCanary(MeshyClient meshClient, Job job) {
        // Strip off preceding slash, if it exists.
        String finalPath = canaryPath.startsWith("/") ? canaryPath.substring(1) : canaryPath;
        long totalBytes = JobAlertUtil.getTotalBytesFromMesh(meshClient, job.getId(), finalPath);
        if (totalBytes < canaryConfigThreshold) {
            return "total bytes: " + totalBytes + " < " + canaryConfigThreshold;
        } else {
            return null;
        }
    }

    /**
     * Returns either a message indicating an error with the configuration
     * or null if the configuration is valid.
     *
     * @return null if configuration is valid.
     */
    @JsonIgnore public String isValid() {
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

}

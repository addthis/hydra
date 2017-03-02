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

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.annotations.Time;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A job alert monitors for specific conditions in the state
 * of one or more hydra jobs. When the condition is met the
 * job alert sends an email to the specified recipients and/or to a webhook.
 * Example conditions are: a job has errored, a job has kicked,
 * the output of a job consists at least X files or bytes,
 * the output of a job follows a specified format, etc.
 *
 * @user-reference
 * @hydra-category Job Alerts
 * @hydra-doc-position 11
 * @exclude-fields activeJobs, activeTriggerTimes, lastAlertTime
 */
@Pluggable("job alerts")
@JsonIgnoreProperties({"alertStatus", "canaryOutputMessage"})
public abstract class AbstractJobAlert implements Codable {

    private static final Logger log = LoggerFactory.getLogger(AbstractJobAlert.class);

    /**
     * This value will be interpreted as "scan through all the jobs in the cluster".
     * It must appear at the only value in the {@code jobIds} field.
     */
    private static final String WILDCARD_JOB_STRING = "*";

    /**
     * How many jobs should be scanned in one iteration of a wildcard job string.
     */
    private static final int WILDCARD_BATCH_SIZE = Parameter.intValue("spawn.alert.batchSize", 50);

    /** Trigger alert if number of consecutive canary check exception is >= this limit */
    private static final int MAX_CONSECUTIVE_CANARY_EXCEPTION = 3;

    @Nonnull @JsonProperty public final String alertId;

    /**
     * Human-readable description of the alert.
     */
    @JsonProperty public final String description;

    /**
     * Optionally specify the number of minutes for the alert
     * to be continuously firing before sending an email or webhook. Can
     * be used to suppress intermittent alerts.
     */
    @JsonProperty public final long delay;

    /**
     * List of email recipients.
     */
    @JsonProperty public final String email;

    /**
     * Webhook URL to receive alert event
     */
    @JsonProperty
    public final String webhookURL;

    /**
     * List of job identifiers.
     */
    @JsonProperty public final ImmutableList<String> jobIds;

    /**
     * If true then rebroadcast the previous error message when
     * an error is detected at the current iteration. Default is false.
     * This is a trade-off where the most recent message for an alert
     * is not emailed with the advantage of no continuous spam of emails.
     */
    @Nonnull @JsonProperty public final SuppressChanges suppressChanges;

    /* Map storing {job id : error description} for all alerted jobs the last time this alert was checked */
    @JsonProperty protected volatile ImmutableMap<String, String> activeJobs;
    /* Map storing {job id : trigger time} for all triggering jobs the last time this alert was checked */
    @JsonProperty protected volatile ImmutableMap<String, Long> activeTriggerTimes;
    // does not distinguish between multiple jobs, and racey wrt activeJobs, but only used for web-ui code for humans
    @JsonProperty protected volatile long lastAlertTime;

    /** Running count of consecutive canary query exceptions. Reset on success. */
    protected final transient AtomicInteger consecutiveCanaryExceptionCount;

    private transient Iterator<Job> streamingIterator;
    private transient boolean alertDisabled;

    private static <K, V> ImmutableMap<K, V> immutableOrEmpty(Map<K, V> input) {
        if (input == null) {
            return ImmutableMap.of();
        } else {
            return ImmutableMap.copyOf(input);
        }
    }

    protected AbstractJobAlert(@Nullable String alertId,
                               String description,
                               @Time(TimeUnit.MINUTES) long delay,
                               String email,
                               String webhookURL,
                               List<String> jobIds,
                               SuppressChanges suppressChanges,
                               long lastAlertTime,
                               Map<String, String> activeJobs,
                               Map<String, Long> activeTriggerTimes) {
        if (alertId == null) {
            String newAlertId = UUID.randomUUID().toString();
            log.debug("creating new alert with uuid: {}", newAlertId);
            this.alertId = newAlertId;
        } else {
            this.alertId = alertId;
        }
        this.description = description;
        this.delay = delay;
        this.email = email;
        this.webhookURL = webhookURL;
        this.jobIds = ImmutableList.copyOf(jobIds);
        this.suppressChanges = suppressChanges;
        this.activeJobs = immutableOrEmpty(activeJobs);
        this.activeTriggerTimes = immutableOrEmpty(activeTriggerTimes);
        this.lastAlertTime = lastAlertTime;
        this.streamingIterator = null;
        this.consecutiveCanaryExceptionCount = new AtomicInteger(0);
        this.setAlertDisabled(false);
    }

    // getters/setters that trigger ser/deser and are not vanilla (also have in-code usages)

    public Map<String, String> getActiveJobs() {
        return activeJobs;
    }

    /** Load state from an existing alert. The provided source alert should not be concurrently modified. */
    public void setStateFrom(AbstractJobAlert sourceAlert) {
        this.lastAlertTime = sourceAlert.lastAlertTime;
        this.activeJobs = sourceAlert.activeJobs;
        this.activeTriggerTimes = sourceAlert.activeTriggerTimes;
    }

    // used by the ui/ web code
    @Deprecated public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    public ImmutableMap<String, String> checkAlertForJobs(Spawn spawn, MeshyClient meshyClient) {
        Set<Job> jobs = getAlertJobs(spawn);
        long now = System.currentTimeMillis();
        long delayMillis = TimeUnit.MINUTES.toMillis(delay);
        ImmutableMap.Builder<String, String> newActiveJobsBuilder = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<String, Long> newActiveTriggerTimesBuilder = new ImmutableMap.Builder<>();
        for (Job job : jobs) {
            long triggerTime = activeTriggerTimes.getOrDefault(job.getId(), now);
            String previousErrorMessage = activeJobs.get(job.getId()); // only interesting for certain edge cases
            String errorMessage = alertActiveForJob(meshyClient, job, previousErrorMessage);
            if (errorMessage != null) {
                newActiveTriggerTimesBuilder.put(job.getId(), triggerTime);
                if ((now - triggerTime) >= delayMillis) {
                    newActiveJobsBuilder.put(job.getId(), errorMessage);
                }
            }
        }
        this.activeTriggerTimes = newActiveTriggerTimesBuilder.build();
        this.activeJobs = newActiveJobsBuilder.build();
        if (activeTriggerTimes.isEmpty()) {
            lastAlertTime = 0;
        } else if (lastAlertTime <= 0) {
            lastAlertTime = System.currentTimeMillis();
        }
        return activeJobs;
    }

    @JsonIgnore
    public final String getTypeString() {
        return getTypeStringInternal() + " ";
    }

    @JsonIgnore
    protected abstract String getTypeStringInternal();

    @VisibleForTesting
    @Nullable
    final String alertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        String validationError = isValid();
        if (validationError != null) {
            return validationError;
        }
        return testAlertActiveForJob(meshClient, job, previousErrorMessage);
    }

    @VisibleForTesting
    @Nullable
    protected abstract String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage);

    @VisibleForTesting
    @Nullable
    protected String handleCanaryException(Exception ex, @Nullable String previousErrorMessage) {
        if ( Throwables.getRootCause(ex) instanceof ConnectException) {
            if (ex.getMessage().contains("HttpHostConnectException")) {
                setAlertDisabled(true);
            }
        }
        // special handling for SocketTimeoutException which is mostly transient
        else if (Throwables.getRootCause(ex) instanceof SocketTimeoutException) {
          int c = consecutiveCanaryExceptionCount.incrementAndGet();
            if (c >= MAX_CONSECUTIVE_CANARY_EXCEPTION) {
                consecutiveCanaryExceptionCount.set(0);
                return "Canary check threw exception at least " + MAX_CONSECUTIVE_CANARY_EXCEPTION + " times in a row. " +
                       "The most recent error is: " + ex;
            } else {
                return previousErrorMessage;
            }
        }
        return ex.toString();
    }

    /**
     * Returns either a message indicating an error with the configuration
     * or null if the configuration is valid.
     *
     * @return null if configuration is valid.
     */
    @JsonIgnore public abstract String isValid();

    @Nonnull private Set<Job> getAlertJobs(Spawn spawn) {
        if (jobIds != null) {
            if (jobIds.size() == 1 && jobIds.get(0).equals(WILDCARD_JOB_STRING)) {
                return streamingJobSet(spawn);
            } else {
                return discreteJobSet(spawn);
            }
        } else {
            return ImmutableSet.of();
        }
    }

    @Nonnull private Set<Job> discreteJobSet(Spawn spawn) {
        Set<Job> rv = new HashSet<>();
        Map<String, List<String>> aliases = spawn.getAliasManager().getAliases();
        for (String lookupId : jobIds) {
            Job job = spawn.getJob(lookupId);
            if (job != null) {
                rv.add(job);
            } else if (aliases.containsKey(lookupId)) {
                for (String jobId : aliases.get(lookupId)) {
                    job = spawn.getJob(jobId);
                    if (job != null) {
                        rv.add(job);
                    }
                }
            }
        }
        return rv;
    }

    @Nonnull private Set<Job> streamingJobSet(Spawn spawn) {
        Set<Job> rv = new HashSet<>();
        if (streamingIterator == null) {
            streamingIterator = spawn.getSpawnState().jobsIterator();
        }
        while (rv.size() < WILDCARD_BATCH_SIZE) {
            if (streamingIterator.hasNext()) {
                rv.add(streamingIterator.next());
            } else {
                streamingIterator = null;
                break;
            }
        }
        for (String lookupId : activeJobs.keySet()) {
            Job job = spawn.getJob(lookupId);
            if (job != null) {
                rv.add(job);
            }
        }
        return rv;
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this);
        } catch (Exception ignored) {
            return super.toString();
        }
    }

    public boolean isAlertDisabled() {
        return alertDisabled;
    }

    public void setAlertDisabled(boolean alertDisabled) {
        this.alertDisabled = alertDisabled;
    }
}

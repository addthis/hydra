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

import java.io.IOException;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.net.http.HttpResponse;
import com.addthis.basis.util.LessStrings;

import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.spawn.SpawnMesh;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.util.EmailUtil;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;

/**
 * This class runs over the set of job alerts, sending trigger/clear emails as appropriate
 */
public class JobAlertRunner {

    private static final Logger log = LoggerFactory.getLogger(JobAlertRunner.class);
    private static final String clusterHead =
            ConfigFactory.load().getString("com.addthis.hydra.job.spawn.Spawn.httpHost");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String meshHost = SpawnMesh.getMeshHost();
    private static final int meshPort = SpawnMesh.getMeshPort();

    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private final Spawn spawn;
    private final SpawnDataStore spawnDataStore;
    private final ConcurrentHashMap<String, AbstractJobAlert> alertMap;

    /**
     * A mapping from (jobIds + aliases) to a set of alertIds.
     * Does not dereference aliases into their corresponding jobIds.
     */
    private final SetMultimap<String, String> jobToAlertsMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private MeshyClient meshyClient;
    private boolean alertsEnabled;
    private volatile boolean lastAlertScanFailed;

    public JobAlertRunner(Spawn spawn, boolean alertEnabled) {
        this.spawn = spawn;
        this.spawnDataStore = spawn.getSpawnDataStore();
        try {
            meshyClient = new MeshyClient(meshHost, meshPort);
        } catch (IOException e) {
            log.warn("Warning: failed to instantiate job alert mesh client", e);
            meshyClient = null;
        }
        this.alertsEnabled = alertEnabled;
        this.alertMap = new ConcurrentHashMap<>();
        loadAlertMap();
    }

    /** Disables alert scanning */
    public void disableAlerts() {
        this.alertsEnabled = false;
    }

    /** Enables alert scanning */
    public void enableAlerts() {
        this.alertsEnabled = true;
    }

    public boolean isAlertsEnabled() {
        return alertsEnabled;
    }

    public boolean isLastAlertScanFailed() {
        return lastAlertScanFailed;
    }

    /**
     * Iterate over alert map, checking the status of each alert and sending emails as needed.
     */
    public void scanAlerts() {
        if (alertsEnabled) {
            log.info("Started alert scan of {} alerts...", alertMap.size());
            try {
                for (Map.Entry<String, AbstractJobAlert> entry : alertMap.entrySet()) {
                    AbstractJobAlert oldAlert = entry.getValue();
                    Map<String, String> currentErrors = oldAlert.getActiveJobs();
                    // entry may be concurrently deleted, so only recompute if still present, and while locked
                    AbstractJobAlert alert = alertMap.computeIfPresent(entry.getKey(), (id, currentAlert) -> {
                        currentAlert.checkAlertForJobs(spawn, meshyClient);
                        if (!currentAlert.getActiveJobs().equals(currentErrors)) {
                            storeAlert(currentAlert.alertId, currentAlert);
                        }
                        return currentAlert;
                    });
                    // null if it was concurrently removed from the map. Does not catch all removals, but might as well
                    // make a best effort attempt to send clears when convenient (should probably move clear emails to
                    // the removal method at some point)
                    if (alert == null) {
                        sendAlert(oldAlert, "[CLEAR] ", currentErrors);
                    } else {
                        Map<String, String> newErrors = alert.getActiveJobs();
                        MapDifference<String, String> difference = Maps.difference(currentErrors, newErrors);
                        sendAlert(oldAlert, "[CLEAR] ", difference.entriesOnlyOnLeft());
                        sendAlert(alert, "[TRIGGER] ", difference.entriesOnlyOnRight());
                        Map<String, String> errorsChanged = new HashMap<>();
                        for (Map.Entry<String, MapDifference.ValueDifference<String>> differing :
                                difference.entriesDiffering().entrySet()) {
                            String oldValue = differing.getValue().leftValue();
                            String newValue = differing.getValue().rightValue();
                            if (!alert.suppressChanges.suppress(oldValue, newValue)) {
                                errorsChanged.put(differing.getKey(), newValue);
                            }
                        }
                        sendAlert(alert, "[ERROR CHANGED] ", errorsChanged);
                    }
                }
                lastAlertScanFailed = false;
                log.info("Finished alert scan");
            } catch (Exception e) {
                lastAlertScanFailed = true;
                log.error("Unexpected error while scanning alerts: {}", e.getMessage(), e);
            }
        }
    }

    private static String emailSummary(Job job) {
        long files = 0;
        double bytes = 0;
        int running = 0;
        int errored = 0;
        int done = 0;
        int numNodes = 0;

        StringBuffer sb = new StringBuffer();

        if (job != null) {

            List<JobTask> jobNodes = job.getCopyOfTasks();

            if (jobNodes != null) {
                numNodes = jobNodes.size();
                for (JobTask task : jobNodes) {
                    files += task.getFileCount();
                    bytes += task.getByteCount();

                    if (!task.getState().equals(JobTaskState.IDLE)) {
                        running++;
                    }
                    switch (task.getState()) {
                        case IDLE:
                            done++;
                            break;
                        case ERROR:
                            done++;
                            errored++;
                            break;
                        default:
                            break;
                    }
                }
            }
            sb.append("Cluster : " + clusterHead + "\n");
            sb.append("Job : " + job.getId() + "\n");
            sb.append("Job Link : http://" + clusterHead + ":5052/spawn2/index.html#jobs/" + job.getId() + "/tasks\n");
            sb.append("Description : " + job.getDescription() + "\n");
            sb.append("------------------------------ \n");
            sb.append("Task Summary \n");
            sb.append("------------------------------ \n");
            sb.append("Job State : " + job.getState() + "\n");
            sb.append("Start Time : " + format(job.getStartTime()) + "\n");
            sb.append("End Time : " + format(job.getEndTime()) + "\n");
            sb.append("Num Nodes : " + numNodes + "\n");
            sb.append("Running Nodes : " + running + "\n");
            sb.append("Errored Nodes : " + errored + "\n");
            sb.append("Done Nodes : " + done + "\n");
            sb.append("Task files : " + files + "\n");
            sb.append("Task Bytes : " + format(bytes) + " GB\n");
            sb.append("------------------------------ \n");
        }
        return sb.toString();
    }

    private static String format(double bytes) {
        double gb = bytes / GIGA_BYTE;

        return decimalFormat.format(gb);
    }

    private static String format(Long time) {
        if (time != null) {
            return dateFormat.format(new Date(time));
        } else {
            return "-";
        }
    }

    private void sendAlert(AbstractJobAlert jobAlert, String reason, Map<String, String> errors) {
        if (errors.isEmpty()) {
            return;
        }

        String alertLink = String.format("http://%s:5052/spawn2/index.html#alerts/%s", clusterHead, jobAlert.alertId);

        log.info("Alerting {} :: jobs : {} : {}", jobAlert.alertId, errors.keySet(), reason);

        if (StringUtils.isNotBlank(jobAlert.email)) {
            sendEmailAlert(jobAlert, alertLink, reason, errors);
        }

        if (StringUtils.isNotBlank(jobAlert.webhookURL)) {
            sendWebhookAlert(jobAlert, alertLink, reason, errors);
        }
    }

    @VisibleForTesting
    static AlertWebhookRequest getWebhookObject(Spawn spawn,
                                                AbstractJobAlert jobAlert,
                                                String alertLink,
                                                String reason,
                                                Map<String, String> errors) {

        // Turn all the jobs in error into a list of information about each job

        AlertWebhookRequest webhookRequest = new AlertWebhookRequest();
        webhookRequest.setAlertLink(alertLink);
        webhookRequest.setAlertType(jobAlert.getTypeString().trim());
        webhookRequest.setAlertReason(reason.trim());
        webhookRequest.setAlertDescription(jobAlert.description);

        errors.forEach((jobUUID, errMsg) -> {

            JobError jobError = new JobError();
            jobError.setId(jobUUID);
            jobError.setError(errMsg);
            jobError.setClusterHead(clusterHead);

            Job job = spawn.getJob(jobUUID);

            if (job != null) {
                jobError.setJobState(job.getState());
                jobError.setDescription(job.getDescription());

                if (job.getStartTime() != null) {
                    jobError.setStartTime(job.getStartTime());
                }

                if (job.getEndTime() != null) {
                    jobError.setEndTime(job.getEndTime());
                }

                List<JobTask> jobTasks = job.getCopyOfTasks();
                jobError.setNodeCount(jobTasks.size());
                jobError.setErrorCount((int) jobTasks.stream().filter(t -> t.getState() == JobTaskState.ERROR).count());
            }

            webhookRequest.getJobsInError().add(jobError);
        });

        return webhookRequest;
    }

    private void sendWebhookAlert(AbstractJobAlert jobAlert,
                                  String alertLink,
                                  String reason,
                                  Map<String, String> errors) {

        try {
            byte[] body = objectMapper.writeValueAsBytes(getWebhookObject(spawn, jobAlert, alertLink, reason, errors));
            HttpResponse response = HttpUtil.httpPost(jobAlert.webhookURL, ContentType.APPLICATION_JSON.getMimeType(), body, 5_000);
            if (response.getStatus() >= 300) {
                log.error("non-200 status code received for webhook alert for alert {}", jobAlert.alertId);
            }
        } catch (IOException ex) {
            log.error("unable to send webhook alert for alert {}", jobAlert.alertId, ex);
        }
    }

    /**
     * Send an email when an alert fires or clears.
     *
     * @param jobAlert The alert to modify
     */
    private void sendEmailAlert(AbstractJobAlert jobAlert,
                                String alertLink,
                                String reason,
                                Map<String, String> errors) {

        String description = jobAlert.description;
        boolean hasDescription = LessStrings.isNotEmpty(description);
        String subject;
        if (hasDescription) {
            subject = reason + ' ' + description.split("\n")[0];
        } else {
            subject = String.format("%s %s - %s - %s", reason, jobAlert.getTypeString(),
                                    JobAlertRunner.getClusterHead(), errors.keySet());
        }

        StringBuilder sb = new StringBuilder(reason + ' ' + jobAlert.getTypeString() + '\n');
        sb.append("Alert link : ").append(alertLink).append('\n');

        if (StringUtils.isNotBlank(description)) {
            sb.append("Alert Description : ").append(description).append('\n');
        }

        for (Map.Entry<String, String> entry : errors.entrySet()) {
            sb.append(emailSummary(spawn.getJob(entry.getKey()))).append('\n');
            sb.append("Error Message\n");
            sb.append(entry.getValue());
            sb.append("\n------------------------------\n");
        }
        if (!EmailUtil.email(jobAlert.email, subject, sb.toString())) {
            log.error("Unable to send email for alert {}", jobAlert.alertId);
        }
    }

    private void loadAlertMap() {
        Map<String, String> alertsRaw = spawnDataStore.getAllChildren(SPAWN_COMMON_ALERT_PATH);
        for (Map.Entry<String, String> entry : alertsRaw.entrySet()) {
            // Underscores are used to mark meta-information (for now, whether we have loaded legacy alerts.)
            if (!entry.getKey().startsWith("_")) {
                loadAlert(entry.getKey(), entry.getValue());
            }
        }
        log.info("{} alerts loaded", alertMap.size());
    }

    private void loadAlert(String id, String raw) {
        try {
            AbstractJobAlert jobAlert = CodecJSON.decodeString(AbstractJobAlert.class, raw);
            alertMap.put(id, jobAlert);
            updateJobToAlertsMap(id, null, jobAlert);
        } catch (Exception ex) {
            log.error("Failed to decode JobAlert id={} raw={}", id, raw, ex);
        }
    }

    /**
     * Remove any outdated mappings from a (job + alias) to an alert and
     * insert new mappings. If {@code old} is null then do not remove
     * any mappings. If {@code alert} is null then do not insert any mappings.
     *
     * @param id    alertId
     * @param old   if non-null then remove associations
     * @param alert if non-null then insert associations
     */
    private void updateJobToAlertsMap(@Nonnull String id, @Nullable AbstractJobAlert old,
                                      @Nullable AbstractJobAlert alert) {
        if (old != null) {
            for (String jobId : old.jobIds) {
                jobToAlertsMap.remove(jobId, id);
            }
        }
        if (alert != null) {
            for (String jobId : alert.jobIds) {
                jobToAlertsMap.put(jobId, id);
            }
        }
    }

    public void putAlert(String id, AbstractJobAlert alert) {
        alertMap.compute(id, (key, old) -> {
            if (old != null) {
                alert.setStateFrom(old);
            }
            updateJobToAlertsMap(id, old, alert);
            storeAlert(id, alert);
            return alert;
        });
    }

    public void removeAlert(String id) {
        if (id != null) {
            alertMap.computeIfPresent(id, (key, value) -> {
                updateJobToAlertsMap(id, value, null);
                storeAlert(id, null);
                return null;
            });
        }
    }

    private void storeAlert(String alertId, @Nullable AbstractJobAlert alert) {
        try {
            if (alert != null) {
                spawnDataStore.putAsChild(SPAWN_COMMON_ALERT_PATH, alertId, CodecJSON.encodeString(alert));
            } else {
                spawnDataStore.deleteChild(SPAWN_COMMON_ALERT_PATH, alertId);
            }
        } catch (Exception e) {
            log.warn("Warning: failed to save alert id={} alert={}", alertId, alert);
        }
    }

    /**
     * Get a snapshot of the alert map as an array, mainly for rendering in the UI.
     *
     * @return A JSONObject representation of all existing alerts
     */
    public JSONArray getAlertStateArray() {
        JSONArray rv = new JSONArray();
        for (AbstractJobAlert jobAlert : alertMap.values()) {
            try {
                rv.put(jobAlert.toJSON());
            } catch (Exception e) {
                log.warn("Warning: failed to send alert in array: {}", jobAlert);
            }
        }
        return rv;
    }

    public JSONObject getAlertStateMap() {
        JSONObject rv = new JSONObject();
        for (AbstractJobAlert jobAlert : alertMap.values()) {
            try {
                rv.put(jobAlert.alertId, jobAlert.toJSON());
            } catch (Exception e) {
                log.warn("Warning: failed to send alert in map: {}", jobAlert);
            }
        }
        return rv;
    }

    public String getAlert(String alertId) {
        try {
            AbstractJobAlert alert = alertMap.get(alertId);
            if (alert == null) {
                return null;
            } else {
                return alert.toJSON().toString();
            }
        } catch (Exception e) {
            log.warn("Failed to fetch alert {}", alertId, e);
            return null;
        }
    }

    public static String getClusterHead() {
        return clusterHead;
    }

    /** Copy and then modify an alert by removing a specific job id. */
    private AbstractJobAlert copyWithoutJobId(@Nonnull String jobId, AbstractJobAlert old) {
        ObjectNode json = Jackson.defaultMapper().valueToTree(old);
        ArrayNode jsonArray = json.putArray("jobIds");
        old.jobIds.stream().filter(x -> !x.equals(jobId)).forEach(jsonArray::add);
        try {
            return Jackson.defaultMapper().treeToValue(json, AbstractJobAlert.class);
        } catch (IOException ex) {
            log.error("Internal error removing job alerts:", ex);
            return old;
        }
    }

    /**
     * Remove {@code jobId} from all alerts that are monitoring it. Delete alerts that are only monitoring this job.
     */
    public void removeAlertsForJob(String jobId) {
        Set<String> alertIds = ImmutableSet.copyOf(jobToAlertsMap.get(jobId));
        for (String mappedAlertId : alertIds) {
            if (alertMap.computeIfPresent(mappedAlertId, (alertId, alert) -> {
                ImmutableList<String> jobIds = alert.jobIds;
                if (jobIds.contains(jobId)) {
                    @Nullable AbstractJobAlert newAlert;
                    if (jobIds.size() == 1) {
                        newAlert = null;
                    } else {
                        newAlert = copyWithoutJobId(jobId, alert);
                    }
                    updateJobToAlertsMap(alertId, alert, newAlert);
                    storeAlert(alertId, newAlert);
                    return newAlert;
                } else {
                    log.warn("jobToAlertsMap has mapping from job {} to alert {} but alert has no reference to job",
                             jobId, alertId);
                    return alert;
                }
            }) == null) {
                log.warn("jobToAlertsMap has mapping from job {} to alert {} but alert does not exist",
                         jobId, mappedAlertId);
            }
        }
    }

    /**
     * Returns alerts for the given job id. Does not look up aliases for a job id. If job id is an alias, will
     * return any alerts that are configured on the alias, but will not look up alerts on the actual job id.
     */
    public Set<AbstractJobAlert> getAlertsForJob(String jobId) {
        Set<String> alertIds = ImmutableSet.copyOf(jobToAlertsMap.get(jobId));
        return alertIds.stream().map(alertMap::get).collect(Collectors.toSet());
    }

    @VisibleForTesting
    static class AlertWebhookRequest {

        private String alertDescription;
        private String alertLink;
        private String alertType;
        private String alertReason;
        private List<JobError> jobsInError = Lists.newArrayList();

        public AlertWebhookRequest() {

        }

        @JsonProperty("alert_description")
        public String getAlertDescription() {
            return alertDescription;
        }

        public void setAlertDescription(String alertDescription) {
            this.alertDescription = alertDescription;
        }

        @JsonProperty("alert_link")
        public String getAlertLink() {
            return alertLink;
        }

        public void setAlertLink(String alertLink) {
            this.alertLink = alertLink;
        }

        @JsonProperty("alert_type")
        public String getAlertType() {
            return alertType;
        }

        public void setAlertType(String alertType) {
            this.alertType = alertType;
        }

        @JsonProperty("alert_reason")
        public String getAlertReason() {
            return alertReason;
        }

        public void setAlertReason(String alertReason) {
            this.alertReason = alertReason;
        }

        @JsonProperty("jobs_in_error")
        public List<JobError> getJobsInError() {
            return jobsInError;
        }

        public void setJobsInError(List<JobError> jobsInError) {
            this.jobsInError = jobsInError;
        }
    }

    @VisibleForTesting
    static class JobError {

        private String id;
        private String description;
        private String clusterHead;
        private String error;
        private JobState jobState;
        private long startTime;
        private long endTime;
        private int nodeCount;
        private int errorCount;

        public JobError() {

        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        @JsonProperty("cluster_head")
        public String getClusterHead() {
            return clusterHead;
        }

        public void setClusterHead(String clusterHead) {
            this.clusterHead = clusterHead;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        @JsonProperty("job_state")
        public JobState getJobState() {
            return jobState;
        }

        public void setJobState(JobState jobState) {
            this.jobState = jobState;
        }

        @JsonProperty("start_time")
        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        @JsonProperty("end_time")
        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        @JsonProperty("node_count")
        public int getNodeCount() {
            return nodeCount;
        }

        public void setNodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @JsonProperty("error_count")
        public int getErrorCount() {
            return errorCount;
        }

        public void setErrorCount(int errorCount) {
            this.errorCount = errorCount;
        }
    }
}

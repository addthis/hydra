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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.LessStrings;

import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.spawn.SpawnMesh;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.util.EmailUtil;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.MeshyClient;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;

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
                        currentAlert.checkAlertForJobs(currentAlert.getAlertJobs(spawn),
                                                       meshyClient);
                        if (!currentAlert.getActiveJobs().equals(currentErrors)) {
                            storeAlert(currentAlert.alertId, currentAlert);
                        }
                        return currentAlert;
                    });
                    // null if it was concurrently removed from the map. Does not catch all removals, but might as well
                    // make a best effort attempt to send clears when convenient (should probably move clear emails to
                    // the removal method at some point)
                    if (alert == null) {
                        emailAlert(oldAlert, "[CLEAR] ", currentErrors);
                    } else {
                        Map<String, String> newErrors = alert.getActiveJobs();
                        MapDifference<String, String> difference = Maps.difference(currentErrors, newErrors);
                        emailAlert(oldAlert, "[CLEAR] ", difference.entriesOnlyOnLeft());
                        emailAlert(alert, "[TRIGGER] ", difference.entriesOnlyOnRight());
                        if (!alert.suppressChanges) {
                            emailAlert(alert, "[ERROR CHANGED] ",
                                       Maps.transformValues(difference.entriesDiffering(),
                                                            MapDifference.ValueDifference::rightValue));
                        }
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

    private static String summary(Job job) {
        long files = 0;
        double bytes = 0;
        int running = 0;
        int errored = 0;
        int done = 0;
        int numNodes = 0;

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

        StringBuffer sb = new StringBuffer();
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

    /**
     * Send an email when an alert fires or clears.
     *
     * @param jobAlert The alert to modify
     */
    private void emailAlert(AbstractJobAlert jobAlert, String reason, Map<String, String> errors) {
        if (errors.isEmpty()) {
            return;
        }
        String subject = String.format("%s %s - %s - %s", reason, jobAlert.getTypeString(),
                                       JobAlertRunner.getClusterHead(), errors.keySet());
        log.info("Alerting {} :: jobs : {} : {}", jobAlert.email, errors.keySet(), reason);
        StringBuilder sb = new StringBuilder(subject + "\n");
        sb.append("Alert link : http://" + clusterHead + ":5052/spawn2/index.html#alerts/" + jobAlert.alertId + "\n");
        String description = jobAlert.description;
        if (LessStrings.isNotEmpty(description)) {
            sb.append("Alert Description : " + description + "\n");
        }
        for (Map.Entry<String, String> entry : errors.entrySet()) {
            sb.append(summary(spawn.getJob(entry.getKey())) + "\n");
            sb.append("Error Message\n");
            sb.append(entry.getValue());
            sb.append("\n------------------------------\n");
        }
        EmailUtil.email(jobAlert.email, subject, sb.toString());
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


}

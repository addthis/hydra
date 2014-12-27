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

import java.io.IOException;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.Strings;

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

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;

/**
 * This class runs over the set of job alerts, sending trigger/clear emails as appropriate
 */
public class JobAlertRunner {

    private static final Logger log = LoggerFactory.getLogger(JobAlertRunner.class);
    private static final String clusterHead = ConfigFactory.load().getString("com.addthis.hydra.job.spawn.Spawn.httpHost");
    private static final String meshHost = SpawnMesh.getMeshHost();
    private static final int meshPort = SpawnMesh.getMeshPort();

    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private final Spawn spawn;
    private final SpawnDataStore spawnDataStore;
    private final ConcurrentHashMap<String, JobAlert> alertMap;
    private final ScheduledThreadPoolExecutor delayedAlertScheduler;

    private MeshyClient meshyClient;
    private boolean alertsEnabled;
    private volatile boolean lastAlertScanFailed;

    private static enum AlertState {
        CLEAR("[CLEAR]"),
        TRIGGER("[TRIGGER]"),
        ERROR_CHANGED("[ERROR CHANGED]");

        private final String message;

        AlertState(String message) {
            this.message = message;
        }

        public String getMessage() { return message; }
    }

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
        this.delayedAlertScheduler = new ScheduledThreadPoolExecutor(1);
        delayedAlertScheduler.setRemoveOnCancelPolicy(true);
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

    private static Predicate<Map.Entry<String,JobAlertUpdate>> generateFilter(JobAlertState state) {
        return (entry -> (entry.getValue().state == state));
    }

    /**
     * Iterate over alert map, checking the status of each alert and sending emails as needed.
     */
    public void scanAlerts() {
        if (alertsEnabled) {
            log.info("Started alert scan of {} alerts...", alertMap.size());
            try {
                for (Map.Entry<String, JobAlert> entry : alertMap.entrySet()) {
                    JobAlert oldAlert = entry.getValue();
                    Map<String, JobAlertUpdate> previousActiveAlerts = oldAlert.getActiveAlerts();
                    // entry may be concurrently deleted, so only recompute if still present, and while locked
                    JobAlert newAlert = alertMap.computeIfPresent(entry.getKey(), (id, previousAlert) -> {
                        previousAlert.checkAlertForJobs(getAlertJobs(previousAlert), meshyClient);
                        if (!previousAlert.getActiveAlerts().equals(previousActiveAlerts)) {
                            storeAlert(previousAlert.alertId, previousAlert);
                        }
                        return previousAlert;
                    });
                    // null if it was concurrently removed from the map. Does not catch all removals, but might as well
                    // make a best effort attempt to send clears when convenient (should probably move clear emails to
                    // the removal method at some point)
                    if (newAlert == null) {
                        emailAlert(oldAlert, AlertState.CLEAR, previousActiveAlerts);
                    } else {
                        Map<String, JobAlertUpdate> newActiveAlerts = newAlert.getActiveAlerts();
                        emailAlert(oldAlert, AlertState.CLEAR, Maps.filterEntries(newActiveAlerts,
                                                      generateFilter(JobAlertState.CLEAR_SENDING_EMAIL)));
                        emailAlert(oldAlert, AlertState.TRIGGER, Maps.filterEntries(newActiveAlerts,
                                                      generateFilter(JobAlertState.TRIGGER_SENDING_EMAIL)));
                        emailAlert(oldAlert, AlertState.ERROR_CHANGED, Maps.filterEntries(newActiveAlerts,
                                                      generateFilter(JobAlertState.TRIGGER_SENDING_CHANGED)));
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

    private Set<Job> getAlertJobs(JobAlert alert) {
        Set<Job> rv = new HashSet<>();
        if (alert != null && alert.jobIds != null) {
            Map<String, List<String>> aliases = spawn.getAliasManager().getAliases();
            for (String lookupId : alert.jobIds) {
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
        }
        return rv;
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

    private String generateEmailSubject(JobAlert jobAlert,
                                        AlertState reason,
                                        Map<String, JobAlertUpdate> errors) {
        return String.format("%s %s - %s - %s", reason.getMessage(), jobAlert.getTypeString(),
                             JobAlertRunner.getClusterHead(), errors.keySet());
    }

    private String generateEmailBody(JobAlert jobAlert, AlertState reason, Map<String, JobAlertUpdate> errors) {
        String subject = generateEmailSubject(jobAlert, reason, errors);
        log.info("Alerting {} :: jobs : {} : {}", jobAlert.email, errors.keySet(), reason.getMessage());
        StringBuilder sb = new StringBuilder(subject + "\n");
        sb.append("Alert link : http://" + clusterHead + ":5052/spawn2/index.html#alerts/" + jobAlert.alertId + "\n");
        String description = jobAlert.description;
        if (Strings.isNotEmpty(description)) {
            sb.append("Alert Description : " + description + "\n");
        }
        for (Map.Entry<String, JobAlertUpdate> entry : errors.entrySet()) {
            sb.append(summary(spawn.getJob(entry.getKey())) + "\n");
            sb.append("Error Message\n");
            sb.append(entry.getValue().error);
            sb.append("\n------------------------------\n");
        }
        return sb.toString();
    }

    /**
     * Delayed alerts are scheduled for delivery on the basis of one email for each
     * job id. If the delayed alert could not be scheduled then send the emails immediately.
     * If the alert is not delayed then send a single email with the status update
     * of all the jobs in this batch.
     *
     * @param jobAlert  job alert to be emailed
     * @param reason    status change of the alert
     * @param errors    map of jobids to error descriptions
     */
    private void emailAlert(JobAlert jobAlert, AlertState reason, Map<String, JobAlertUpdate> errors) {
        if (errors.isEmpty()) {
            return;
        }
        String subject = generateEmailSubject(jobAlert, reason, errors);
        String message = generateEmailBody(jobAlert, reason, errors);
        EmailUtil.email(jobAlert.email, subject, message);
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
            JobAlert jobAlert = CodecJSON.decodeString(JobAlert.class, raw);
            alertMap.put(id, jobAlert);
        } catch (Exception ex) {
            log.error("Failed to decode JobAlert id={} raw={}", id, raw, ex);
        }
    }

    public void putAlert(String id, JobAlert alert) {
        alertMap.compute(id, (key, old) -> {
            if (old != null) {
                alert.setActiveAlerts(old.getActiveAlerts());
            }
            storeAlert(id, alert);
            return alert;
        });
    }

    public void removeAlert(String id) {
        if (id != null) {
            alertMap.computeIfPresent(id, (key, value) -> {
                spawnDataStore.deleteChild(SPAWN_COMMON_ALERT_PATH, id);
                return null;
            });
        }
    }

    private void storeAlert(String alertId, JobAlert alert) {
        try {
            spawnDataStore.putAsChild(SPAWN_COMMON_ALERT_PATH, alertId, CodecJSON.encodeString(alert));
        } catch (Exception e) {
            log.warn("Warning: failed to save alert id={} alert={}", alertId, alert);
        }
    }

    /**
     * Get a snapshot of the alert map as an array, mainly for rendering in the UI.
     * @return A JSONObject representation of all existing alerts
     */
    public JSONArray getAlertStateArray() {
        JSONArray rv = new JSONArray();
        for (JobAlert jobAlert : alertMap.values()) {
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
        for (JobAlert jobAlert : alertMap.values()) {
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
            JobAlert alert = alertMap.get(alertId);
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
}

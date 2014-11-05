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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

import com.google.common.collect.MapDifference;
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

    private MeshyClient meshyClient;
    private boolean alertsEnabled;

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

    /**
     * Method that disables alert scanning
     */
    public void disableAlerts() {
        this.alertsEnabled = false;
    }

    /**
     * Method that enables alert scanning
     */
    public void enableAlerts() {
        this.alertsEnabled = true;
    }

    /**
     * Iterate over alert map, checking the status of each alert and sending emails as needed.
     */
    public void scanAlerts() {
        if (alertsEnabled) {
            for (Map.Entry<String, JobAlert> entry : alertMap.entrySet()) {
                JobAlert oldAlert = entry.getValue();
                Map<String, String> currentErrors = oldAlert.getActiveJobs();
                JobAlert alert = alertMap.computeIfPresent(entry.getKey(), (id, currentAlert) -> {
                    currentAlert.checkAlertForJobs(getAlertJobs(currentAlert), meshyClient);
                    if (!currentAlert.getActiveJobs().equals(currentErrors)) {
                        storeAlert(currentAlert.alertId, currentAlert);
                    }
                    return currentAlert;
                });
                if (alert == null) {
                    emailAlert(oldAlert, "[CLEAR] ", currentErrors);
                } else {
                    Map<String, String> newErrors = alert.getActiveJobs();
                    MapDifference<String, String> difference = Maps.difference(currentErrors, newErrors);
                    emailAlert(oldAlert, "[CLEAR] ", difference.entriesOnlyOnLeft());
                    emailAlert(alert, "[TRIGGER] ", difference.entriesOnlyOnRight());
                    emailAlert(alert, "[ERROR CHANGED] ",
                               Maps.transformValues(difference.entriesDiffering(),
                                                    MapDifference.ValueDifference::rightValue));
                }
            }
        }
    }

    private List<Job> getAlertJobs(JobAlert alert) {
        List<Job> rv = new ArrayList<>();
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

    /**
     * Send an email when an alert fires or clears.
     * @param jobAlert The alert to modify
     */
    private void emailAlert(JobAlert jobAlert, String reason, Map<String, String> errors) {
        if (errors.isEmpty()) {
            return;
        }
        String subject = String.format("%s %s - %s - %s", reason, jobAlert.getTypeString(),
                                       JobAlertRunner.getClusterHead(), errors.keySet());
        log.info("Alerting {} :: jobs : {} : {}", jobAlert.email, errors.keySet(), reason);
        StringBuilder sb = new StringBuilder(subject + "\n");
        sb.append("Alert link : http://" + clusterHead + ":5052/spawn2/index.html#alerts/" + jobAlert.alertId + "\n");
        String description = jobAlert.description;
        if (Strings.isNotEmpty(description)) {
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
            JobAlert jobAlert = CodecJSON.decodeString(JobAlert.class, raw);
            alertMap.put(id, jobAlert);
        } catch (Exception ex) {
            log.error("Failed to decode JobAlert id={} raw={}", id, raw, ex);
        }
    }

    public void putAlert(String id, JobAlert alert) {
        alertMap.compute(id, (key, old) -> {
            if (old != null) {
                alert.setActiveJobs(old.getActiveJobs());
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

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.*;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.util.EmailUtil;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.addthis.meshy.MeshyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_LOADED_LEGACY;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;

/**
 * This class runs over the set of job alerts, sending trigger/clear emails as appropriate
 */
public class JobAlertRunner {

    private static final Logger log = LoggerFactory.getLogger(JobAlertRunner.class);
    private static String clusterHead = Spawn.getHttpHost();
    private final Spawn spawn;
    private final SpawnDataStore spawnDataStore;

    private static final long ALERT_REPEAT_MILLIS = Parameter.longValue("spawn.job.alert.repeat", 5 * 60 * 1000);
    private static final long ALERT_DELAY_MILLIS = Parameter.longValue("spawn.job.alert.delay", 60 * 1000);

    private static final String meshHost = SpawnMesh.getMeshHost();
    private static final int meshPort = SpawnMesh.getMeshPort();
    private MeshyClient meshyClient;

    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private final ConcurrentHashMap<String, JobAlert> alertMap;

    private static final CodecJSON codec = new CodecJSON();

    private boolean alertsEnabled;


    public JobAlertRunner(Spawn spawn) {
        this.spawn = spawn;
        this.spawnDataStore = spawn.getSpawnDataStore();
        try {
            meshyClient = new MeshyClient(meshHost, meshPort);
        } catch (IOException e) {
            log.warn("Warning: failed to instantiate job alert mesh client", e);
            meshyClient = null;
        }
        Timer alertTimer = new Timer("JobAlertTimer");
        this.alertsEnabled = spawn.areAlertsEnabled();
        alertTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                scanAlerts();
            }
        }, ALERT_DELAY_MILLIS, ALERT_REPEAT_MILLIS);
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
            synchronized (alertMap) {
                for (Map.Entry<String, JobAlert> entry : alertMap.entrySet()) {
                    checkAlert(entry.getValue());
                }
            }
        }
    }

    /**
     * Check whether a particular alert should fire or clear based on job states
     * @param alert The actual alert object
     */
    private void checkAlert(JobAlert alert) {
        boolean alertHasChanged = alert.checkAlertForJobs(getAlertJobs(alert), meshyClient);
        if (alertHasChanged) {
            emailAlert(alert);
        }
    }

    private List<Job> getAlertJobs(JobAlert alert) {
        List<Job> rv = new ArrayList<>();
        if (alert != null && alert.getJobIds() != null) {
            Map<String, List<String>> aliases = spawn.getAliases();
            for (String lookupId : alert.getJobIds()) {
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
                }
            }
        }

        StringBuffer sb = new StringBuffer();
        sb.append("Cluster : " + clusterHead + "\n");
        sb.append("Job : " + job.getId() + "\n");
        sb.append("Link : http://" + clusterHead + ":5052/spawn2/index.html#jobs/" + job.getId() + "/tasks\n");
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
    private void emailAlert(JobAlert jobAlert) {
        String status = jobAlert.getAlertStatus();
        Map<String, String> activeJobs = jobAlert.getActiveJobs();
        log.info("Alerting " + jobAlert.getEmail() + " :: jobs : " + activeJobs.keySet() + " : " + status);
        StringBuilder sb = new StringBuilder();
        sb.append("Alert: " + jobAlert.getAlertStatus() + " \n");
        for (String jobId : activeJobs.keySet()) {
            sb.append(summary(spawn.getJob(jobId)) + "\n");
        }
        EmailUtil.email(jobAlert.getEmail(), status, sb.toString());
        putAlert(jobAlert.getAlertId(), jobAlert);
    }

    private void loadAlertMap() {
        synchronized (alertMap) {
            Map<String, String> alertsRaw = spawnDataStore.getAllChildren(SPAWN_COMMON_ALERT_PATH);
            for (Map.Entry<String, String> entry : alertsRaw.entrySet()) {
                // Underscores are used to mark meta-information (for now, whether we have loaded legacy alerts.)
                if (!entry.getKey().startsWith("_")) {
                    loadAlert(entry.getKey(), entry.getValue());
                }
            }
            if (spawnDataStore.get(SPAWN_COMMON_ALERT_LOADED_LEGACY) == null) {
                // One time only, iterate through jobs looking for their alerts.
                try {
                    loadLegacyAlerts();
                    spawnDataStore.put(SPAWN_COMMON_ALERT_LOADED_LEGACY, "1");
                } catch (Exception ex) {
                    log.warn("Warning: failed to fetch legacy alerts", ex);
                }

            }
        }
    }

    private void loadAlert(String id, String raw) {
        try {
            JobAlert jobAlert = codec.decode(new JobAlert(), raw.getBytes());
            synchronized (alertMap) {
                alertMap.put(id, jobAlert);
            }
        } catch (Exception ex) {
            log.warn("Failed to decode JobAlert id=" + id + " raw=" + raw);
        }
    }

    public void putAlert(String id, JobAlert alert) {
        synchronized (alertMap) {
            JobAlert oldAlert = alertMap.containsKey(id) ? alertMap.get(id) : null; // ConcurrentHashMap throws NPE on failed 'get'
            if (oldAlert != null) {
                // Inherit old alertTime even if the alert has changed in other ways
                alert.setLastAlertTime(oldAlert.getLastAlertTime());
                alert.setActiveJobs(oldAlert.getActiveJobs());
            }
            alertMap.put(id, alert);
        }
        storeAlert(id, alert);
    }

    public void removeAlert(String id) {
        if (id != null) {
            synchronized (alertMap) {
                alertMap.remove(id);
                spawnDataStore.deleteChild(SPAWN_COMMON_ALERT_PATH, id);
            }
        }
    }

    /**
     * A method called one time to retrieve any job alerts that were saved within Job objects
     */
    private void loadLegacyAlerts() {
        List<JobAlert> alerts;
        spawn.acquireJobLock();
        try {
            synchronized (alertMap) {
                for (Job job : spawn.listJobs()) {
                    if (job != null && (alerts = job.getAlerts()) != null) {
                        for (JobAlert alert : alerts) {
                            alert.setJobIds(new String[] {job.getId()});
                            String newUUID = UUID.randomUUID().toString();
                            alert.setAlertId(newUUID);
                            alertMap.put(newUUID, alert);
                            storeAlert(newUUID, alert);
                        }
                        job.setAlerts(null);
                    }
                }
            }
        }  finally {
            spawn.releaseJobLock();
        }

    }

    private void storeAlert(String alertId, JobAlert alert) {
        synchronized (alertMap) {
            try {
                spawnDataStore.putAsChild(SPAWN_COMMON_ALERT_PATH, alertId, new String(codec.encode(alert)));
            } catch (Exception e) {
                log.warn("Warning: failed to save alert id=" + alertId + " alert=" + alert);
            }
        }
    }

    /**
     * Get a snapshot of the alert map as an array, mainly for rendering in the UI.
     * @return A JSONObject representation of all existing alerts
     */
    public JSONArray getAlertStateArray() {
        JSONArray rv = new JSONArray();
        synchronized (alertMap) {
            for (JobAlert jobAlert : alertMap.values()) {
                try {
                    rv.put(jobAlert.toJSON());
                } catch (Exception e) {
                    log.warn("Warning: failed to send alert in array: " + jobAlert);
                }
            }
        }
        return rv;
    }

    public JSONObject getAlertStateMap() {
        JSONObject rv = new JSONObject();
        synchronized (alertMap) {
            for (JobAlert jobAlert : alertMap.values()) {
                try {
                    rv.put(jobAlert.getAlertId(), jobAlert.toJSON());
                } catch (Exception e) {
                    log.warn("Warning: failed to send alert in map: " + jobAlert);
                }

            }
        }
        return rv;
    }

    public String getAlert(String alertId) {
        synchronized (alertMap) {
            try {
                return alertMap.containsKey(alertId) ? alertMap.get(alertId).toJSON().toString() : null;
            } catch (Exception e) {
                log.warn("Failed to fetch alert " + alertId, e);
                return null;
            }
        }
    }

    public static String getClusterHead() {
        return clusterHead;
    }

}

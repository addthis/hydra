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
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.util.EmailUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_LOADED_LEGACY;
import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;

/**
 * This class runs a timer that scans the jobs for any email alerts and sends them.
 */
public class JobAlertRunner {

    private static final Logger log = LoggerFactory.getLogger(JobAlertRunner.class);
    private static String clusterName = Parameter.value("spawn.localhost", "unknown");
    private final Timer alertTimer;
    private final Spawn spawn;
    private final SpawnDataStore spawnDataStore;

    private static final long REPEAT = 5 * 60 * 1000;
    private static final long DELAY = 5 * 60 * 1000;

    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final int MINUTE = 60 * 1000;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private final DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private final ConcurrentHashMap<String, JobAlert> alertMap;

    private static final CodecJSON codec = new CodecJSON();

    private boolean alertsEnabled;


    public JobAlertRunner(Spawn spawn) {
        this.spawn = spawn;
        this.spawnDataStore = spawn.getSpawnDataStore();
        alertTimer = new Timer("JobAlertTimer");
        this.alertsEnabled = spawn.areAlertsEnabled();
        alertTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                scanAlerts();
            }
        }, DELAY, REPEAT);
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
     * Method that loads jobs from Spawn and scans the alerts for jobs that have alerts. It checks to see if the condition for the alert
     * exists and it sends the email if does, if it doesn't then it clears the alert.
     */
    public void scanAlerts() {
        // Only scan jobs for alerts if alerting is enabled.
        if (alertsEnabled) {
            log.info("Running alert scan ..........");
            for (Job job : this.spawn.listJobs()) {
                //boolean used to track whether the job needs to be updated
                boolean alertsChanged = false;
                List<JobAlert> alerts = job.getAlerts();
                if (alerts != null) {
                    for (JobAlert jobAlert : alerts) {
                        long currentTime = JitterClock.globalTime();
                        if (jobAlert.isOnError()) {
                            if (job.getState().equals(JobState.ERROR)) {
                                if (!jobAlert.hasAlerted()) {
                                    emailAlert(job, "Task is in Error ", jobAlert, false);
                                    alertsChanged = true;
                                }
                            } else {
                                if (jobAlert.hasAlerted()) {
                                    alertsChanged = true;
                                    emailAlert(job, "{CLEAR} Task is in Error ", jobAlert, true);
                                }
                            }
                        }
                        int jobTimeout = jobAlert.getTimeout();
                        String timeUnit = (jobTimeout == 1) ? " minute" : " minutes";
                        if (jobAlert.isOnComplete()) {
                            // job is idle and has completed within the last 60 minutes
                            if (job.getState().equals(JobState.IDLE)) {
                                if (!jobAlert.hasAlerted()) {
                                    emailAlert(job, "Task has Completed ", jobAlert, false);
                                    alertsChanged = true;
                                }
                            } else {
                                if (jobAlert.hasAlerted()) {
                                    alertsChanged = true;
                                    emailAlert(job, "{CLEAR} Task has Completed ", jobAlert, true);
                                }
                            }
                        } else if (jobAlert.isRuntimeExceeded()) {
                            if (job.getState().equals(JobState.RUNNING) && (job.getSubmitTime() != null) &&
                                ((currentTime - job.getSubmitTime()) > jobTimeout * MINUTE)) {
                                if (!jobAlert.hasAlerted()) {
                                    emailAlert(job, "Task runtime has exceed : " + jobTimeout + timeUnit, jobAlert, false);
                                    alertsChanged = true;
                                }
                            } else {
                                if (jobAlert.hasAlerted()) {
                                    alertsChanged = true;
                                    emailAlert(job, "{CLEAR} Task runtime has exceed : " + jobTimeout + timeUnit, jobAlert, true);
                                }
                            }
                        } else if (jobAlert.isRekickTimeout()) {
                            if (!job.getState().equals(JobState.RUNNING) && (job.getEndTime() != null) &&
                                ((currentTime - job.getEndTime()) > jobTimeout * MINUTE)) {
                                if (!jobAlert.hasAlerted()) {
                                    emailAlert(job, "Task has not been re-kicked in : " + jobTimeout + timeUnit, jobAlert, false);
                                    alertsChanged = true;
                                }
                            } else {
                                if (jobAlert.hasAlerted()) {
                                    alertsChanged = true;
                                    emailAlert(job, "{CLEAR} Task has not been re-kicked in : " + jobTimeout + timeUnit, jobAlert, true);
                                }
                            }
                        }
                    }
                    //alert state for job has changed, so we need to ask spawn to update job
                    if (alertsChanged) {
                        this.spawn.updateJobAlerts(job.getId(), alerts);
                    }
                }
            }
        }

    }

    /**
     * Creates a summary message for the alert
     *
     * @param job
     * @param msg
     * @return
     */
    private String summary(Job job, String msg) {
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
        sb.append("Cluster : " + clusterName + "\n");
        sb.append("Job : " + job.getId() + "\n");
        sb.append("Link : http://" + clusterName + ":5052/spawn2/index.html#jobs/" + job.getId() + "/tasks\n");
        sb.append("Alert : " + msg + "\n");
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

    private String format(double bytes) {
        double gb = bytes / GIGA_BYTE;

        return decimalFormat.format(gb);
    }

    private String format(Long time) {
        if (time != null) {
            return dateFormat.format(new Date(time));
        } else {
            return "-";
        }
    }

    /**
     * Emails a message, Only the body can contain newline characters.
     *
     * @param job
     * @param message
     */
    private void emailAlert(Job job, String message, JobAlert jobAlert, boolean clear) {
        log.info("Alerting " + jobAlert.getEmail() + " :: job : " + job.getId() + " : " + message);
        String subject = message + " - " + clusterName + " - " + job.getDescription();
        if (clear) {
            jobAlert.clear();
        } else {
            jobAlert.alerted();
        }
        EmailUtil.email(jobAlert.getEmail(), subject, summary(job, message));
    }

    private void loadAlertMap() {
        synchronized (alertMap) {
            Map<String, String> alertsRaw = spawnDataStore.getAllChildren(SPAWN_COMMON_ALERT_PATH);
            for (Map.Entry<String, String> entry : alertsRaw.entrySet()) {
                loadAlert(entry.getKey(), entry.getValue());
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
            alertMap.put(id, jobAlert);
        } catch (Exception ex) {
            log.warn("Failed to decode JobAlert id=" + id + " raw=" + raw);
        }
    }

    public void putAlert(String id, JobAlert alert) {
        alertMap.put(id, alert);
        storeAlerts();
    }

    public void removeAlert(String id) {
        if (id != null) {
            alertMap.remove(id);
            spawnDataStore.deleteChild(SPAWN_COMMON_ALERT_PATH, id);
            storeAlerts();
        }
    }

    private void loadLegacyAlerts() {
        List<JobAlert> alerts;
        spawn.acquireJobLock();
        try {
            for (Job job : spawn.listJobs()) {
                if (job != null && (alerts = job.getAlerts()) != null) {
                    for (JobAlert alert : alerts) {
                        alertMap.put(UUID.randomUUID().toString(), alert);
                    }
                    job.setAlerts(null);
                }
            }
            storeAlerts();
        }  finally {
            spawn.releaseJobLock();
        }

    }

    private void storeAlerts() {
        synchronized (alertMap) {
            for (Map.Entry<String, JobAlert> entry : alertMap.entrySet()) {
                try {
                    spawnDataStore.putAsChild(SPAWN_COMMON_ALERT_PATH, entry.getKey(), new String(codec.encode(entry.getValue())));
                } catch (Exception e) {
                    log.warn("Warning: failed to save alert id=" + entry.getKey() + " alert=" + entry.getValue());
                }
            }
        }
    }


}

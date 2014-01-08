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
import java.util.Timer;
import java.util.TimerTask;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskState;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.util.EmailUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class runs a timer that scans the jobs for any email alerts and sends them.
 */
public class JobAlertRunner {

    private static final Logger log = LoggerFactory.getLogger(JobAlertRunner.class);
    private static final long REPEAT = 5 * 60 * 1000;
    private static final long DELAY = 5 * 60 * 1000;
    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final int MINUTE = 60 * 1000;
    private static String clusterName = Parameter.value("spawn.localhost", "unknown");
    private final Timer alertTimer;
    private final Spawn spawn;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private final DecimalFormat decimalFormat = new DecimalFormat("#.###");
    private boolean alertsEnabled;


    public JobAlertRunner(Spawn spawn) {
        this.spawn = spawn;
        alertTimer = new Timer("JobAlertTimer");
        this.alertsEnabled = spawn.areAlertsEnabled();
        alertTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                scanAlerts();
            }
        }, DELAY, REPEAT);
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

    private String format(Long time) {
        if (time != null) {
            return dateFormat.format(new Date(time));
        } else {
            return "-";
        }
    }

    private String format(double bytes) {
        double gb = bytes / GIGA_BYTE;

        return decimalFormat.format(gb);
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


}

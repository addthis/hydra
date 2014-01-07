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

import com.addthis.hydra.job.IJob;
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
    private static final String CLUSTER_NAME = Parameter.value("spawn.localhost", "unknown");
    private static final long REPEAT = 5 * 60 * 1000;
    private static final long DELAY = 5 * 60 * 1000;
    private static final long GIGA_BYTE = (long) Math.pow(1024, 3);
    private static final int MINUTE = 60 * 1000;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmm");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#.###");

    private final Spawn spawn;

    private boolean alertsEnabled;


    public JobAlertRunner(Spawn spawn) {
        this.spawn = spawn;
        Timer alertTimer = new Timer("JobAlertTimer");
        this.alertsEnabled = spawn.areAlertsEnabled();
        alertTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                scanAlerts();
            }
        }, DELAY, REPEAT);
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
                List<JobAlert> alerts = job.getAlerts();
                if (alerts != null) {
                    boolean alertsChanged = false;
                    for (JobAlert jobAlert : alerts) {
                        long currentTime = JitterClock.globalTime();
                        if (jobAlert.isOnError()) {
                            if (job.getState() == JobState.ERROR) {
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
                            if (job.getState() == JobState.IDLE) {
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
                            if ((job.getState() == JobState.RUNNING) && (job.getSubmitTime() != null) &&
                                ((currentTime - job.getSubmitTime()) > (jobTimeout * MINUTE))) {
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
                            if ((job.getState() != JobState.RUNNING) && (job.getEndTime() != null) &&
                                ((currentTime - job.getEndTime()) > (jobTimeout * MINUTE))) {
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
    private static void emailAlert(IJob job, String message, JobAlert jobAlert, boolean clear) {
        log.info("Alerting {} :: job : {} : {}", jobAlert.getEmail(), job.getId(), message);
        String subject = message + " - " + CLUSTER_NAME + " - " + job.getDescription();
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
    private static String summary(IJob job, String msg) {
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

                if (task.getState() != JobTaskState.IDLE) {
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
                        // ignored
                }
            }
        }

        StringBuilder sb = new StringBuilder(300);
        sb.append("Cluster : ").append(CLUSTER_NAME).append("\n");
        sb.append("Job : ").append(job.getId()).append("\n");
        sb.append("Link : http://").append(CLUSTER_NAME).append(":5052/spawn2/index.html#jobs/").append(job.getId()).append("/tasks\n");
        sb.append("Alert : ").append(msg).append("\n");
        sb.append("Description : ").append(job.getDescription()).append("\n");
        sb.append("------------------------------ \n");
        sb.append("Task Summary \n");
        sb.append("------------------------------ \n");
        sb.append("Job State : ").append(job.getState()).append("\n");
        sb.append("Start Time : ").append(format(job.getStartTime())).append("\n");
        sb.append("End Time : ").append(format(job.getEndTime())).append("\n");
        sb.append("Num Nodes : ").append(numNodes).append("\n");
        sb.append("Running Nodes : ").append(running).append("\n");
        sb.append("Errored Nodes : ").append(errored).append("\n");
        sb.append("Done Nodes : ").append(done).append("\n");
        sb.append("Task files : ").append(files).append("\n");
        sb.append("Task Bytes : ").append(format(bytes)).append(" GB\n");
        sb.append("------------------------------ \n");
        return sb.toString();
    }

    private static String format(Long time) {
        if (time == null) {
            return "-";
        }
        return dateFormat.format(new Date(time));
    }

    private static String format(double bytes) {
        double gb = bytes / GIGA_BYTE;
        return decimalFormat.format(gb);
    }
}

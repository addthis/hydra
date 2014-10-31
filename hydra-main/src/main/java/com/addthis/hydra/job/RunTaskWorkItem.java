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
package com.addthis.hydra.job;

import java.io.File;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.job.minion.JobTask;
import com.addthis.hydra.job.minion.MinionWorkItem;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunTaskWorkItem extends MinionWorkItem {
    private static final Logger log = LoggerFactory.getLogger(RunTaskWorkItem.class);

    private static final Meter autoRetryMeter = Metrics.newMeter(RunTaskWorkItem.class,
                                                                 "autoRetries", "autoRetries", TimeUnit.HOURS);

    private Integer port = null;
    private int maxStops = 2;

    private final boolean autoRetry;

    public RunTaskWorkItem(File pidFile,
                           File runFile,
                           File doneFile,
                           JobTask task,
                           boolean execute,
                           boolean autoRetry) {
        super(pidFile, runFile, doneFile, task, execute);
        this.autoRetry = autoRetry;
    }

    @Override
    public void updateStats() {
        task.updateFileStats();
    }

    @Override
    public long getStartTime() {
        return task.getStartTime();
    }

    @Override
    public void setStartTime(long start) {
        task.setStartTime(start);
    }

    @Override
    public void sendFinishStatusMessages(int exit) throws Exception {
        task.unmonitor();
        if (exit == 0) {
            synchronized (task) {
                task.execReplicate(null, null, false, true, false);
            }
        } else {
            task.sendEndStatus(exit);
        }
    }

    @Override
    public void executeWaitingCommands() {
        if (port == null) {
            port = task.getPort();
            if (port != null) {
                task.sendPort();
            }
        }
        long runtime = task.getKick().getRunTime();
        if ((runtime > 0) && ((System.currentTimeMillis() - getStartTime()) > runtime)) {
            log.warn("[exit.wait] time stop {} @ {}", task.getName(), runtime);
            if (maxStops > 0) {
                task.stopWait(false);
            }
            maxStops--;
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                log.warn("[exit.wait] time stop interrupted", e);
            }
        }
    }

    /**
     * If a task has autoRetry enabled, sometimes revert + retry to get around transient errors. The conditions under
     * which or the number of times that revert + retry may be performed is undefined and may vary over time. AutoRetry
     * should only be enabled on jobs where this is acceptable.
     */
    @Override
    public int waitForProcessExit() throws Exception {
        int lastExit = waitAndGetExit();
        if (autoRetry
            // Only auto retry for exit codes that java returns for JVM errors (128 + 6 (SIGABRT))
            && (lastExit == 134)
            // Do not retry if the task was manually killed
            && !task.wasStopped()) {
            List<String> backups = task.getBackupsOrdered();
            // After failing at least once, put more info into the minion log
            log.warn("[exit.wait] attempting retry for {} due to failed exit={}", task.getName(), lastExit);
            if (!backups.isEmpty()) {
                String backupName = backups.get(0);
                log.warn("[exit.wait] restoring {} to {} and retrying, delete={}",
                         task.getJobDir(), backupName, doneFile.delete());
                File backupDir = new File(task.getJobDir().getParentFile(), backupName);
                if (task.promoteBackupToLive(backupDir, task.getLiveDir())) {
                    autoRetryMeter.mark();
                    startAndWaitForPid();
                    lastExit = waitAndGetExit();
                } else {
                    log.warn("cancelling retry for {} due to failed revert", task.getName());
                }
            } else {
                log.warn("[exit.wait] exhausted backups for {}; sending error code", task.getName());
            }
        }
        return lastExit;
    }

    private int waitAndGetExit() {
        // Wait for the job.done to exist and attempt to parse the exit code
        String exitString = exitWait();
        if (exitString != null) {
            return getExitStatusFromString(exitString);
        } else {
            log.warn("{} exited with null", task.getName());
            return -1;
        }
    }

    @Override
    public void clear() {
        log.warn("[task.clear] {}", task.getName());
        if (doneFile.exists() && (getStartTime() > 0)) {
            task.setRuntime(doneFile.lastModified() - getStartTime());
        }
        setStartTime(0);
        task.setProcess(null);
        task.save();
    }
}

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

import com.addthis.hydra.job.minion.JobTask;
import com.addthis.hydra.job.minion.MinionWorkItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunTaskWorkItem extends MinionWorkItem {

    private static final Logger log = LoggerFactory.getLogger(RunTaskWorkItem.class);
    private Integer port = null;
    private int maxStops = 2;
    private int retries;

    public RunTaskWorkItem(File pidFile,
                           File runFile,
                           File doneFile,
                           JobTask task,
                           boolean execute,
                           int retries) {
        super(pidFile, runFile, doneFile, task, execute);
        this.retries = retries;
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
     * If a task has retries specified, revert further and further back until the task gets exit=0 (success)
     * or all retries have been exhausted.
     */
    @Override
    public int waitForProcessExit() {
        int lastExit = 0;
        List<String> backups = task.getBackupsOrdered();
        // When retries=0, the following loop should be run exactly once (thus the <=)
        for (int i = 0; i <= retries; i++) {
            if (i > 0) {
                // After failing at least once, put more info into the minion log
                log.warn("[exit.wait] attempting retry #{} for {} due to failed exit={}", i, task.getName(), lastExit);
                if (i >= backups.size()) {
                    log.warn("[exit.wait] exhausted backups for {}; sending error code", task.getName());
                    break;
                } else {
                    String backupName = backups.get(i);
                    log.warn("[exit.wait] restoring {} to {} and retrying, delete={}",
                             task.getJobDir(), backupName, doneFile.delete());
                    File backupDir = new File(task.getJobDir().getParentFile(), backupName);
                    task.promoteBackupToLive(backupDir, task.getLiveDir());
                }
            }
            // Wait for the job.done to exist and attempt to parse the exit code
            String exitString = exitWait();
            if (exitString != null) {
                lastExit = getExitStatusFromString(exitString);
            } else {
                log.warn("{} exited with null", task.getName());
            }
            // Do not retry if the task was manually killed
            if ((lastExit == 0) || task.wasStopped()) {
                return lastExit;
            }
        }
        return lastExit;
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

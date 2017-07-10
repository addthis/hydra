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
package com.addthis.hydra.minion;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.List;

import com.addthis.hydra.job.JobTaskErrorCode;
import com.addthis.hydra.job.mq.CommandTaskRevert;
import com.addthis.hydra.job.mq.CoreMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandTaskRevertRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandTaskRevertRunner.class);
    String rootDir = "";    // todo: will be removed when merge to master( just for testing )
    private Minion minion;
    CoreMessage core;

    public CommandTaskRevertRunner(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskRevert revert = (CommandTaskRevert) core;
        List<JobTask> match = minion.getMatchingJobs(revert);
        log.warn("[task.revert] request " + revert.getJobKey() + " matched " + match.size());

        if (match.size() == 0 && revert.getNodeID() != null && revert.getNodeID() >= 0) {
            log.warn("[task.revert] unmatched for " + revert.getJobUuid() + " / " + revert.getNodeID());
        }
        if (revert.getNodeID() == null || revert.getNodeID() < 0) {
            log.warn("[task.revert] got invalid node id " + revert.getNodeID());
            return;
        }

        for (JobTask task : match) {
            if (task.isRunning() || task.isReplicating() || task.isBackingUp()) {
                log.warn("[task.revert] " + task.getJobKey() + " skipped. job node active.");
            } else {
                long time = System.currentTimeMillis();
                task.setReplicas(revert.getReplicas());
                if (revert.getSkipMove()) {
                    try {
                        task.execReplicate(null, null, false, true, false);
                    } catch (Exception ex) {
                        task.sendEndStatus(JobTaskErrorCode.EXIT_REVERT_FAILURE);
                    }
                } else {
                    String dir = rootDir + task.taskRoot.toString();
                    if(hasBackup(dir, task.taskRoot) > 0) {
                        task.revertToBackup(revert.getRevision(), revert.getTime(), revert.getBackupType());
                        log.warn("[task.revert] " + task.getJobKey() + " completed in " + (System.currentTimeMillis() - time) + "ms.");
                    } else {
                        String jobId = task.getJobKey().getJobUuid().toString();
                        runCommand(rootDir + minion.getRootDir().getPath().toString(), "rm -rf " + jobId);
                        log.warn("[task.revert] " + task.getJobKey() + " has NO any backup and remove {} directory", jobId);
                    }
                }
            }
        }
        minion.writeState();
    }

    private int hasBackup(String dir, File taskRootFile) {
        String taskRootFileString = taskRootFile.toString();
        int count = 0;
        count += runCommand(dir, "ls -d b-hourly-*");
        count += runCommand(dir, "ls -d b-daily-*");
        count += runCommand(dir, "ls -d b-weekly-*");
        count += runCommand(dir, "ls -d b-monthly-*");
        return count;
    }

    private int runCommand(String path, String cmd) {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("bash", "-c", cmd);
        File dir = new File(path);
        builder.directory(dir);

        Process process = null;
        try {
            process = builder.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        int lineCount = 0;
        try {
            while (reader.readLine()!= null) {
                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineCount;
    }
}

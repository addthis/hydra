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
package com.addthis.hydra.job.minion;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.mq.CommandTaskKick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MinionWorkItem implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MinionWorkItem.class);
    protected File pidFile;
    private File runFile;
    protected File doneFile;
    private boolean execute;
    protected final JobTask task;

    public MinionWorkItem(File pidFile, File runFile, File doneFile, JobTask task, boolean execute) {
        this.pidFile = pidFile;
        this.runFile = runFile;
        this.doneFile = doneFile;
        this.task = task;
        this.execute = execute;
    }

    public abstract long getStartTime();

    public abstract void setStartTime(long start);

    private final int numPidFileTries = Parameter.intValue("minion.pid.file.retries", 1200);

    /**
     * Send out messages indicating the workitem has ended, based on the exit code
     *
     * @param exit The exit code: 0 for success, >0 for (possible) failure
     * @throws Exception If messaging fails for whatever reason
     */
    public abstract void sendFinishStatusMessages(int exit) throws Exception;

    /**
     * Commands to run while waiting for the workitem to finish -- for example, sending the port number to spawn
     */
    public abstract void executeWaitingCommands();

    /**
     * Clear this workitem, blanking its start time and saving the Minion.JobTask
     */
    public abstract void clear();

    /**
     * Update any relevant job stats (like total byte size in RunTaskWorkItem)
     */
    public void updateStats() {
    }

    /**
     * Run this workitem, including retries if appropriate
     */
    @Override public void run() {
        int exit = -1;
        task.allocate();
        boolean interrupted = false;
        try {
            startAndWaitForPid();
            exit = waitForProcessExit();
        } catch (InterruptedException ie) {
            interrupted = true;
        } catch (Exception e) {
            log.warn("{} exception during script execution: {}", task.getName(), e, e);
            } finally {
            task.deallocate();
            try {
                if (!interrupted) {
                    sendFinishStatusMessages(exit);
                }
            } catch (Exception ex) {
                log.warn("{} exception when sending exit status: {}", task.getName(), ex, ex);
            }
        }
        log.warn("{} exited with {}", task.getName(), exit);
    }

    protected void startAndWaitForPid() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        if (execute) {
            task.setProcess(Runtime.getRuntime().exec("sh " + runFile));
        }
        for (int j = 0; j < numPidFileTries && !pidFile.exists(); j++) {
            // Watch for the pid file. If it still doesn't exist after some time, fail noisily.
            Thread.sleep(1000);
        }
        if (!pidFile.exists()) {
            // We must interrupt the task process so that the replicate won't suddenly kick in at a future time
            task.interruptProcess();
            String msg = "failed to find pid file for " + task.getName() + " at " + pidFile + " after waiting";
            throw new RuntimeException(msg);
        }
        long waited = System.currentTimeMillis() - start;
        String pid = null;
        try {
            pid = LessBytes.toString(LessFiles.read(pidFile)).trim();
        } catch (FileNotFoundException ex) {
            log.warn("{} pid file not found", task.getName());
        }
        if (waited > 500) {
            log.warn("{} pid [{}] after {}ms", task.getName(), pid, waited);
        }
        log.debug("{} waiting for exit pid={}", task.getName(), pid);
    }

    protected int waitForProcessExit() throws Exception {
        int exit = 0;
        String exitString = exitWait();
        if (exitString != null) {
            exit = getExitStatusFromString(exitString);
        } else {
            log.warn("{} exited with null", task.getName());
        }
        return exit;
    }

    /**
     * Wait for the workitem to finish, as indicated by the doneFile
     *
     * @return The exit code, assuming one was received
     */
    public String exitWait() {
        String name = task.getName();
        CommandTaskKick kick = task.getKick();
        if (kick != null) {
            log.warn("[exit.wait] {} maxTime={} start={}", name, kick.getRunTime(), getStartTime());
        }
        try {
            while (!task.isDeleted() && !doneFile.exists() && doneFile.getParentFile().exists()) {
                Thread.sleep(100);
                executeWaitingCommands();
                task.createDoneFileIfNoProcessRunning(pidFile, doneFile);
            }
            Thread.sleep(100);
            return LessBytes.toString(LessFiles.read(doneFile)).trim();

        } catch (Exception ex)  {
            log.warn("", ex);
        } finally {
            updateStats();
            clear();
            removeFromTask();
        }
        return null;
    }

    private void removeFromTask() {
        task.setWorkItemThread(null);
    }

    public int getExitStatusFromString(String exitString) {
        int exit;
        try {
            exit = Integer.parseInt(exitString);
        } catch (NumberFormatException ne) {
            if (exitString == null) {
                exit = 1337;
            } else {
                exit = 0;
            }
        }
        // re-map 143 (exited on kill -1) to success
        if (exit == 143) {
            exit = 0;
        }
        return exit;
    }

}

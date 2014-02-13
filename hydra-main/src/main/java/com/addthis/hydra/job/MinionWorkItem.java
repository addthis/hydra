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
import java.io.FileNotFoundException;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.job.mq.CommandTaskKick;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public abstract class MinionWorkItem implements Runnable {

    private static Logger log = LoggerFactory.getLogger(MinionWorkItem.class);
    protected File pidFile;
    private File runFile;
    protected File doneFile;
    private File logOut;
    private File logErr;
    private boolean execute;
    protected final Minion.JobTask task;

    public MinionWorkItem(File jobDir, File pidFile, File runFile, File doneFile, Minion.JobTask task, boolean execute) {
        this.pidFile = pidFile;
        this.runFile = runFile;
        this.doneFile = doneFile;
        this.task = task;
        File logRoot = new File(jobDir, "log");
        logOut = new File(logRoot, "log.out");
        logErr = new File(logRoot, "log.err");
        this.execute = execute;
    }

    public abstract String getLogPrefix();

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
    public void run() {
        int exit = -1;
        String logPrefix = getLogPrefix();
        task.allocate();
        boolean interrupted = false;
        try {
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
                pid = Bytes.toString(Files.read(pidFile)).trim();
            } catch (FileNotFoundException ex) {
                log.warn(logPrefix + " " + task.getName() + " pid file not found");
            }
            if (waited > 500) {
                log.warn(logPrefix + " " + task.getName() + " pid [" + pid + "] after " + waited + "ms");
            }
            if (log.isDebugEnabled()) {
                log.debug(logPrefix + " " + task.getName() + " waiting for exit pid=" + pid);
            }
            exit = waitForProcessExit();
        } catch (InterruptedException ie) {
            interrupted = true;
        } catch (Exception e) {
            log.warn(logPrefix + " " + task.getName() + " exception during script execution: " + e, e);
            } finally {
            task.deallocate();
            try {
                if (!interrupted) {
                    sendFinishStatusMessages(exit);
                }
            } catch (Exception ex) {
                log.warn(logPrefix + " " + task.getName() + " exception when sending exit status: " + ex, ex);
                }
        }
        log.warn(logPrefix + " " + task.getName() + " exited with " + exit);
    }

    protected int waitForProcessExit() {
        int exit = 0;
        String exitString = exitWait();
        if (exitString != null) {
            exit = getExitStatusFromString(exitString);
        } else {
            log.warn(getLogPrefix() + " " + task.getName() + " exited with null");
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
            log.warn("[exit.wait] " + name + " maxTime=" + kick.getRunTime() + " start=" + getStartTime());
        }
        try {
            Minion.FileWatcher stdoutWatch = null;
            Minion.FileWatcher stderrWatch = null;
            if (kick != null && !Strings.isEmpty(kick.getKillSignal())) {
                stdoutWatch = new Minion.FileWatcher(logOut, kick.getKillSignal());
                stderrWatch = new Minion.FileWatcher(logErr, kick.getKillSignal());
            }
            while (!task.isDeleted() && !doneFile.exists() && doneFile.getParentFile().exists()) {
                Thread.sleep(100);
                executeWaitingCommands();
                if (stdoutWatch != null && stdoutWatch.containsKill()) {
                    log.warn("[exit.wait] stdout kill " + name);
                    task.stopWait(true);
                }
                if (stderrWatch != null && stderrWatch.containsKill()) {
                    log.warn("[exit.wait] stderr kill " + name);
                    task.stopWait(true);
                }
                task.createDoneFileIfNoProcessRunning(pidFile, doneFile);
            }
            Thread.sleep(100);
            return Bytes.toString(Files.read(doneFile)).trim();

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

    protected static int getExitStatusFromString(String exitString) {
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

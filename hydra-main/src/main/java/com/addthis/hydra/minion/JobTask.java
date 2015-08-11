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

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.LessStrings;
import com.addthis.basis.util.SimpleExec;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.BackupWorkItem;
import com.addthis.hydra.job.JobTaskErrorCode;
import com.addthis.hydra.job.ReplicateWorkItem;
import com.addthis.hydra.job.RunTaskWorkItem;
import com.addthis.hydra.job.backup.DailyBackup;
import com.addthis.hydra.job.backup.GoldBackup;
import com.addthis.hydra.job.backup.HourlyBackup;
import com.addthis.hydra.job.backup.MonthlyBackup;
import com.addthis.hydra.job.backup.ScheduledBackupType;
import com.addthis.hydra.job.backup.WeeklyBackup;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CommandTaskNew;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.mq.ReplicaTarget;
import com.addthis.hydra.job.mq.StatusTaskBackup;
import com.addthis.hydra.job.mq.StatusTaskBegin;
import com.addthis.hydra.job.mq.StatusTaskEnd;
import com.addthis.hydra.job.mq.StatusTaskPort;
import com.addthis.hydra.job.mq.StatusTaskReplica;
import com.addthis.hydra.job.mq.StatusTaskReplicate;
import com.addthis.hydra.job.mq.StatusTaskRevert;
import com.addthis.hydra.task.run.TaskExitState;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties("retries")
public class JobTask implements Codable {
    private static final Logger log = LoggerFactory.getLogger(JobTask.class);

    @FieldConfig(required = true) String id;
    @FieldConfig(required = true) Integer node;
    @FieldConfig(required = true) int runCount;
    @FieldConfig(required = true) long runTime;

    @FieldConfig Integer nodeCount;
    @FieldConfig CommandTaskKick kick;
    @FieldConfig long startTime;
    @FieldConfig boolean monitored = true;
    @FieldConfig long fileCount;
    @FieldConfig long fileBytes;
    @FieldConfig volatile boolean deleted;
    @FieldConfig boolean autoRetry;
    @FieldConfig boolean wasQueued;
    @FieldConfig long replicateStartTime;
    @FieldConfig long backupStartTime;
    @FieldConfig String rebalanceSource;
    @FieldConfig String rebalanceTarget;

    @FieldConfig volatile ReplicaTarget[] failureRecoveryReplicas;
    @FieldConfig volatile ReplicaTarget[] replicas;

    Minion minion;
    Process process;
    Thread workItemThread;
    File taskRoot;
    File jobRun;
    File replicateSH;
    File replicateRun;
    File backupSH;
    File backupRun;
    File jobDone;
    File replicateDone;
    File backupDone;
    File jobStopped;
    File jobDir;
    File logDir;
    File logOut;
    File logErr;
    File jobPid;
    File replicatePid;
    File backupPid;
    File jobPort;
    Integer port;

    public JobTask(Minion minion) {this.minion = minion;}

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public JobKey getJobKey() {
        return new JobKey(id, node);
    }

    public CommandTaskKick getKick() {
        return kick;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getReplicateStartTime() {
        return replicateStartTime;
    }

    public void setReplicateStartTime(long replicateStartTime) {
        this.replicateStartTime = replicateStartTime;
    }

    public long getBackupStartTime() {
        return backupStartTime;
    }

    public void setBackupStartTime(long backupStartTime) {
        this.backupStartTime = backupStartTime;
    }

    public void setProcess(Process p) {
        this.process = p;
    }

    public void interruptProcess() {
        if (this.process != null) {
            this.process.destroy();
        }
    }

    public boolean getAutoRetry() {
        return autoRetry;
    }

    public void setAutoRetry(boolean autoRetry) {
        this.autoRetry = autoRetry;
    }

    public void clearFailureReplicas() {
        // Merge all failureRecoveryReplicas into the master list, then clear failureRecoveryReplicas
        List<ReplicaTarget> finalReplicas = replicas != null ? new ArrayList<>(Arrays.asList(replicas)) :
                                            new ArrayList<>();
        if (failureRecoveryReplicas != null) {
            finalReplicas.addAll(Arrays.asList(failureRecoveryReplicas));
        }
        replicas = finalReplicas.toArray(new ReplicaTarget[finalReplicas.size()]);
        failureRecoveryReplicas = new ReplicaTarget[]{};
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setWorkItemThread(MinionWorkItem workItemThread) {
        this.workItemThread = workItemThread == null ? null : new Thread(workItemThread);
    }

    public void save() {
        try {
            LessFiles.write(new File(getConfigDir(), "job.state"), LessBytes.toBytes(CodecJSON.encodeString(this)), false);
        } catch (Exception e) {
            log.warn("", e);
        }
    }

    public void updateFileStats() {
        final TimerContext updateTimer = minion.fileStatsTimer.time();
        FileStats stats = new FileStats();
        stats.update(jobDir);
        try {
            LessFiles.write(new File(getConfigDir(), "job.stats"), LessBytes.toBytes(CodecJSON.encodeString(stats)), false);
        } catch (Exception e) {
            log.warn("", e);
        }
        fileCount = stats.count;
        fileBytes = stats.bytes;
        updateTimer.stop();
    }

    public void allocate() {
        Minion.capacityLock.lock();
        try {
            minion.activeTaskKeys.add(this.getName());
        } finally {
            Minion.capacityLock.unlock();
        }
    }

    public void deallocate() {
        Minion.capacityLock.lock();
        try {
            minion.activeTaskKeys.remove(this.getName());
        } finally {
            Minion.capacityLock.unlock();
        }
    }

    public void sendNewStatusToReplicaHost(String hostUUID) {
        minion.sendControlMessage(new CommandTaskNew(hostUUID, getJobKey().getJobUuid(), getJobKey().getNodeNumber()));
    }

    public void sendEndStatus(int exit) {
        sendEndStatus(exit, null, null);
    }

    public void sendEndStatus(int exit, String rebalanceSource, String rebalanceTarget) {
        TaskExitState exitState = new TaskExitState();
        File jobExit = new File(jobDir, "job.exit");
        if (jobExit.exists() && jobExit.canRead()) {
            try {
                CodecJSON.INSTANCE.decode(exitState, LessFiles.read(jobExit));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        exitState.setWasStopped(wasStopped());
        StatusTaskEnd end = new StatusTaskEnd(minion.uuid, id, node, exit, fileCount, fileBytes);
        end.setRebalanceSource(rebalanceSource);
        end.setRebalanceTarget(rebalanceTarget);
        end.setWasQueued(wasQueued);
        end.setExitState(exitState);
        setRebalanceSource(null);
        setRebalanceTarget(null);
        minion.sendStatusMessage(end);
        try {
            minion.kickNextJob();
        } catch (Exception e) {
            log.warn("[task.kick] exception while trying to kick next job", e);
        }
    }

    public void sendPort() {
        minion.sendStatusMessage(new StatusTaskPort(minion.uuid, kick.getJobUuid(), kick.getNodeID(), port));
    }

    /* restore a job state from a job/task-id root directory */
    boolean restoreTaskState(File taskDir) throws IOException {
        taskRoot = taskDir;
        File liveDir = new File(taskDir, "live");
        File replicaDir = new File(taskDir, "replica");
        File configDir = new File(taskDir, "config");
        LessFiles.initDirectory(configDir);
        String jobID = taskDir.getParentFile().getName();
        String nodeID = taskDir.getName();
        String taskPath = jobID + "/" + nodeID;
        if (replicaDir.isDirectory()) {
            // Cut over replica to live
            replicaDir.renameTo(liveDir);
        } else if (liveDir.exists() && !minion.liveEverywhereMarkerFile.exists()) {
            // On first startup, mark any existing "live" directory as complete.
            new File(liveDir, "replicate.complete").createNewFile();
        }
        if (!liveDir.isDirectory()) {
            log.warn("[restore] {} has no live or replica directories", taskPath);
            return false;
        }
        id = jobID;
        node = Integer.parseInt(nodeID);
        initializeFileVariables();
        if (!minion.liveEverywhereMarkerFile.exists()) {
            // On first startup, make sure to get to known idle state
            jobDone.createNewFile();
            backupDone.createNewFile();
            replicateDone.createNewFile();
        }
        File jobState = new File(configDir, "job.state");
        if (jobState.exists()) {
            try {
                CodecJSON.decodeString(this, LessBytes.toString(LessFiles.read(jobState)));
            } catch (Exception e) {
                log.warn("", e);
                return false;
            }
        }
        if (Integer.parseInt(nodeID) != node) {
            log.warn("[restore] {} mismatch with node # {}", taskPath, node);
            return false;
        }
        if (!jobID.equals(id)) {
            log.warn("[restore] {} mismatch with node id {}", taskPath, id);
            return false;
        }
        monitored = true;
        recoverWorkItem();
        return true;
    }

    /* If minion detects that a task was running when the minion was shut down, attempt to recover by looking for the pid */
    private void recoverWorkItem() {
        try {
            if ((startTime > 0) && (replicateStartTime == 0) && (backupStartTime == 0)) {
                log.warn("[restore] {} as running", getName());
                exec(this.kick, false);
            } else if ((replicateStartTime > 0) && (backupStartTime == 0)) {
                log.warn("[restore] {} as replicating", getName());
                execReplicate(null, null, false, false, false);
            } else if (isBackingUp()) {
                log.warn("[restore] {} as backing up", getName());
                execBackup(null, null, false);
            } else if ((startTime > 0) || (replicateStartTime > 0) || (backupStartTime > 0)) {
                // Minion had a process running that finished during the downtime; notify Spawn
                log.warn("[restore] {} as previously active; now finished", getName());
                startTime = 0;
                replicateStartTime = 0;
                backupStartTime = 0;
                sendEndStatus(0);
            }
        } catch (Exception ex) {
            log.warn("WARNING: failed to restore state for {}", getName(), ex);
        }

    }

    void initializeFileVariables() {
        jobDir = getLiveDir();
        File configDir = getConfigDir();
        logDir = new File(jobDir, "log");
        logOut = new File(logDir, "log.out");
        logErr = new File(logDir, "log.err");
        jobPid = new File(configDir, "job.pid");
        replicatePid = new File(configDir, "replicate.pid");
        backupPid = new File(configDir, "backup.pid");
        jobPort = new File(jobDir, "job.port");
        jobDone = new File(configDir, "job.done");
        replicateDone = new File(configDir, "replicate.done");
        backupDone = new File(configDir, "backup.done");
    }

    boolean isComplete() {
        File replicaComplete = new File(getLiveDir(), "replicate.complete");
        return replicaComplete.exists();
    }

    private boolean shouldExecuteReplica(ReplicaTarget replica) {
        if (replica.getHostUuid().equals(minion.uuid)) {
            log.warn("Host: {} received a replication target of itself, this is NOT allowed for {}", minion.uuid,
                     getName());
            return false;
        }
        return true;
    }

    private List<String> assembleReplicateCommandAndInformSpawn(ReplicaTarget replica, boolean replicateAllBackups) throws IOException {
        List<String> rv = new ArrayList<>();
        if (replica == null || !shouldExecuteReplica(replica)) {
            return null;
        }
        try {
            String target = ProcessUtils.getTaskBaseDir(replica.getBaseDir(), id, node);
            if (!replicateAllBackups) {
                target += "/live";
            }
            String userAT = replica.getUserAT();
            String mkTarget = Minion.remoteConnectMethod + " " + userAT + " mkdir -p " + target + "/";
            log.warn("[replicate] {} to {}:{}", getJobKey(), userAT, target);
            if (log.isDebugEnabled()) {
                log.debug(" --> {}", mkTarget);
            }
            int runCount = kick != null ? kick.getRunCount() : 0;
            minion.sendStatusMessage(
                    new StatusTaskReplica(replica.getHostUuid(), id, node, runCount, System.currentTimeMillis()));
            rv.add(mkTarget);
            if (replicateAllBackups) {
                StringBuilder sb = new StringBuilder();
                sb.append(createRsyncCommand(userAT, jobDir.getParentFile().getAbsolutePath() + "/", target));
                for (String backup : findLocalBackups(true)) {
                    if (backup.startsWith(ScheduledBackupType.getBackupPrefix())) {
                        // only include "b-" dirs/exclude gold - it won't exist on the remote host after the rsync.
                        // On some occasions, this logic can attempt to touch a backup that is about to be deleted -- if so, log a message but don't fail the command
                        sb.append("\n" + createTouchCommand(false, userAT, target + "/" + backup + "/backup.complete", true));
                    }
                }
                sb.append("\n" + createTouchCommand(false, userAT, target + "/live/replicate.complete", false));
                rv.add(sb.toString());
            } else {
                rv.add(createDeleteCommand(false, userAT, target + "/replicate.complete") +
                                "\n" + createRsyncCommand(userAT, jobDir.getAbsolutePath() + "/", target) +
                                "\n" + createTouchCommand(false, userAT, target + "/replicate.complete", false)
                );
            }
        } catch (Exception ex) {
            log.warn("failed to replicate {} to {}", this.getJobKey(), replica.getHost(), ex);
        }
        return rv;
    }

    private List<String> assembleBackupCommandsForHost(boolean local, ReplicaTarget replica, List<String> symlinkCommands, List<String> deleteCommands, long time) {
        List<String> copyCommands = new ArrayList<>();
        for (ScheduledBackupType type : ScheduledBackupType.getBackupTypes().values()) {
            String[] allBackups = local ? findLocalBackups(false) : findRemoteBackups(false, replica);
            String backupName = type.generateNameForTime(time, true);
            String symlinkName = type.getSymlinkName();
            String userAT = local ? null : replica.getUserAT();
            String source = "live";
            String path = local ? jobDir.getParentFile().getAbsolutePath() : ProcessUtils.getTaskBaseDir(
                    replica.getBaseDir(),
                    id, node);
            int maxNumBackups = getMaxNumBackupsForType(type);
            if (maxNumBackups > 0 && type.shouldMakeNewBackup(allBackups)) {
                String backupCMD = createBackupCommand(local, userAT, path, source, backupName);
                copyCommands.add(backupCMD);
                if (symlinkName != null) {
                    symlinkCommands.add(createSymlinkCommand(local, userAT, path, backupName, symlinkName));
                }
                maxNumBackups -= 1; // Diminish the max number by one, because we're about to add a new one
            }
            List<String> backupsToDelete = type.oldBackupsToDelete(allBackups, allBackups, maxNumBackups);
            for (String oldBackup : backupsToDelete) {
                if (MinionTaskDeleter.shouldDeleteBackup(oldBackup, type)) {
                    deleteCommands.add(createDeleteCommand(local, userAT, path + "/" + oldBackup));
                }
            }
        }
        minion.writeState();
        return copyCommands;
    }

    private String createRsyncCommand(String userAT, String source, String target) throws Exception {
        return "retry " + Minion.rsyncCommand + (Minion.copyBandwidthLimit > 0 ? " --bwlimit " + Minion.copyBandwidthLimit : "") + " -Hqa --exclude config --exclude gold --exclude replicate.complete --exclude backup.complete --delete-after -e \\'" + Minion.remoteConnectMethod + "\\' " + source + " " + userAT + ":" + target;
    }

    private String createBackupCommand(boolean local, String userAT, String baseDir, String source, String name) {
        String sourceDir = baseDir + "/" + source;
        String targetDir = baseDir + "/" + name;
        log.warn("[backup] executing backup from {} to {}", sourceDir, targetDir);
        return createDeleteCommand(local, userAT, targetDir) + " && " +
                createCopyCommand(local, userAT, sourceDir, targetDir) + " && " +
                createTouchCommand(local, userAT, targetDir + "/backup.complete", false);
    }

    private String createSymlinkCommand(boolean local, String userAt, String baseDir, String source, String name) {
        String linkDir = baseDir + "/" + name;
        String tmpName = linkDir + "_tmp";
        return wrapCommandWithRetries(local, userAt, "if [ ! -L " + linkDir + " ]; then rm -rf " + linkDir + " ; fi && " + MacUtils.lncmd + " -nsf " + source + " " + tmpName + " && " + MacUtils.mvcmd + " -Tf " + tmpName + " " + linkDir);
    }

    private String createCopyCommand(boolean local, String userAt, String sourceDir, String targetDir) {
        String cpParams = MacUtils.linkBackup ? " -lr " : " -r ";
        return wrapCommandWithRetries(local, userAt, MacUtils.cpcmd + cpParams + sourceDir + " " + targetDir);
    }

    private String createTouchCommand(boolean local, String userAT, String path, boolean failSafe) {
        return wrapCommandWithRetries(local, userAT, "touch " + path + (failSafe ? " 2>/dev/null || echo 'Skipped deleted backup'" : ""));
    }

    private String createDeleteCommand(boolean local, String userAT, String dirPath) {
        return wrapCommandWithRetries(local, userAT, MacUtils.rmcmd + " -rf " + dirPath);
    }

    private String wrapCommandWithRetries(boolean local, String userAt, String command) {
        return "retry \"" + (local ? "" : Minion.remoteConnectMethod + " " + userAt + " '") + command + (local ? "" : "'") + "\"";
    }

    /**
     * Find local backups for a task.
     *
     * @param completeOnly Whether to restrict to backups that contain the backup.complete file
     * @return A list of directory names
     */
    private String[] findLocalBackups(boolean completeOnly) {
        File[] dirs = jobDir.getParentFile().listFiles();
        if (dirs == null) {
            return new String[]{};
        }
        List<String> rvList = new ArrayList<>();
        for (File dir : dirs) {
            if (dir.isDirectory()) {
                if (!completeOnly || LessStrings.contains(dir.list(), "backup.complete")) {
                    rvList.add(dir.getName());
                }
            }
        }
        Collections.sort(rvList);
        return rvList.toArray(new String[]{});
    }

    /**
     * Find backups for a task on a replica host
     *
     * @param completeOnly Whether to restrict to backups that contain the backup.complete file
     * @param replica      The ReplicaTarget object describing the destination for this replica
     * @return A list of directory names
     */
    private String[] findRemoteBackups(boolean completeOnly, ReplicaTarget replica) {
        try {
            String userAT = replica.getUser() + "@" + replica.getHost();
            String baseDir = ProcessUtils.getTaskBaseDir(replica.getBaseDir(), id, node);
            if (completeOnly) {
                baseDir += "/*/backup.complete";
            }
            String lsResult = execCommandReturnStdOut(
                    Minion.remoteConnectMethod + " " + userAT + " " + MacUtils.lscmd + " " + baseDir);
            String[] lines = lsResult.split("\n");
            if (completeOnly) {
                List<String> rv = new ArrayList<>(lines.length);
                for (String line : lines) {
                    String[] splitLine = line.split("/");
                    if (splitLine.length > 2) {
                        rv.add(splitLine[splitLine.length - 2]);
                    }
                }
                return rv.toArray(new String[]{});
            } else {
                return lines;
            }
        } catch (Exception ex) {
            return new String[]{};
        }
    }

    private String execCommandReturnStdOut(String sshCMD) throws InterruptedException, IOException {
        String[] wrappedCMD = new String[]{"/bin/sh", "-c", sshCMD};
        SimpleExec command = runCommand(wrappedCMD, null);
        if (command.exitCode() == 0) {
            return command.stdoutString();
        } else {
            return "";
        }
    }

    private SimpleExec runCommand(String[] sshCMDArray, String sshCMD) throws InterruptedException, IOException {
        SimpleExec command;
        if (sshCMD != null) {
            command = new SimpleExec(sshCMD).join();
        } else {
            command = new SimpleExec(sshCMDArray).join();
        }
        return command;
    }

    /* Read the proper number of backups for each type from the kick parameters */
    private int getMaxNumBackupsForType(ScheduledBackupType type) {
        if (type instanceof GoldBackup) {
            return 3; // Keep 3 gold backups around so that these directories will linger for query/streaming stability
        }
        if (kick == null) {
            return -1; // If we're not sure how many backups to create, hold off until we receive a task kick
        }
        if (type instanceof HourlyBackup) {
            return kick.getHourlyBackups();
        } else if (type instanceof DailyBackup) {
            return kick.getDailyBackups();
        } else if (type instanceof WeeklyBackup) {
            return kick.getWeeklyBackups();
        } else if (type instanceof MonthlyBackup) {
            return kick.getMonthlyBackups();
        } else {
            return 0; // Unknown backup type
        }
    }

    /**
     * Move the specified backup dir onto the live dir
     *
     * @param backupDir The "good" version of a task
     * @param targetDir The target directory, generally "live", which may have bad/incomplete data
     * @return True if the operation succeeds
     */
    public boolean promoteBackupToLive(File backupDir, File targetDir) {
        if (targetDir != null && backupDir != null && backupDir.exists() && backupDir.isDirectory()) {
            moveAndDeleteAsync(targetDir);
            // Copy the backup directory onto the target directory
            String cpCMD = MacUtils.cpcmd + (MacUtils.linkBackup ? " -lrfT " : " -rfT ");
            return ProcessUtils.shell(cpCMD + backupDir + " " + targetDir + " >> /dev/null 2>&1", minion.rootDir) == 0;
        } else {
            log.warn("[restore] invalid backup dir {}", backupDir);
        }
        return false;
    }

    /**
     * Move a file to a temporary location, then delete it asynchronously via a request to MinionTaskDeleter
     *
     * @param file The file to be deleted.
     */
    private void moveAndDeleteAsync(File file) {
        if (file != null && file.exists()) {
            File tmpLocation = new File(file.getParent(), "BAD-" + System.currentTimeMillis());
            if (file.renameTo(tmpLocation)) {
                copyLogBackAsArchive(file.toPath(), tmpLocation.toPath(), Paths.get("log/log.out"));
                copyLogBackAsArchive(file.toPath(), tmpLocation.toPath(), Paths.get("log/log.err"));
                submitPathToDelete(tmpLocation.getPath());
            } else {
                throw new RuntimeException("Could not rename file for asynchronous deletion: " + file);
            }
        }
    }

    private void copyLogBackAsArchive(Path oldName, Path newName, Path logSubpath) {
        try {
            Path newLogPath = newName.resolve(logSubpath);
            if (java.nio.file.Files.isSymbolicLink(newLogPath)) {
                Path resolvedPath = newLogPath.toRealPath();
                Path oldLogPath = oldName.resolve(logSubpath).getParent()
                                         .resolve(resolvedPath.getFileName().toString() + ".bad");
                java.nio.file.Files.createDirectories(oldLogPath.getParent());
                java.nio.file.Files.move(resolvedPath, oldLogPath);
            }
        } catch (Exception ex) {
            log.warn("exception while trying to preserve reverted task's old logs -- ignoring", ex);
        }
    }

    private void submitPathToDelete(String path) {
        minion.minionStateLock.lock();
        try {
            minion.minionTaskDeleter.submitPathToDelete(path);
        } finally {
            minion.minionStateLock.unlock();
        }
    }

    public boolean revertToBackup(int revision, long time, String type) {
        Minion.revertLock.lock();
        try {
            if (isRunning() || isReplicating() || isBackingUp()) {
                log.warn("[revert] cannot promote backup for active task {}", getName());
                return false;
            }
            ScheduledBackupType typeToUse = ScheduledBackupType.getBackupTypes().get(type);
            if (typeToUse == null) {
                log.warn("[revert] unrecognized backup type {}", type);
                return false;
            }
            String backupName;
            if (revision < 0) {
                backupName = getBackupByTime(time, type);
            } else {
                backupName = getBackupByRevision(revision, type);
            }
            if (backupName == null) {
                log.warn("[revert] found no backups of type {} and time {} to revert to for {}; failing",
                         type, time, getName());
                return false;
            }
            File oldBackup = new File(jobDir.getParentFile(), backupName);
            log.warn("[revert] {} from {}", getName(), oldBackup);
            minion.sendStatusMessage(new StatusTaskRevert(minion.getUUID(), id, node));
            boolean promoteSuccess = promoteBackupToLive(oldBackup, jobDir);
            if (promoteSuccess) {
                try {
                    execReplicate(null, null, false, true, false);
                    return true;
                } catch (Exception ex) {
                    log.warn("[revert] post-revert replicate of {} failed", getName(), ex);
                    return false;
                }
            } else {
                log.warn("[revert] {} from {} failed", getName(), oldBackup);
                sendEndStatus(JobTaskErrorCode.EXIT_REVERT_FAILURE);
                return false;
            }
        } finally {
            Minion.revertLock.unlock();
        }
    }

    private String getBackupByTime(long time, String type) {
        ScheduledBackupType backupType = ScheduledBackupType.getBackupTypes().get(type);
        String[] backups = findLocalBackups(true);
        if (backups == null || backups.length == 0) {
            log.warn("[revert] fail, there are no local backups of type {} for {}", type, getName());
            return null;
        }
        String timeName = backupType.stripSuffixAndPrefix(backupType.generateNameForTime(time, true));
        for (String backupName : backups) {
            if (backupType.isValidName(backupName) && (backupType.stripSuffixAndPrefix(backupName).equals(timeName))) {
                return backupName;
            }
        }
        log.warn("[revert] fail, invalid backup time for {}: {}", getName(), time);
        return null;
    }

    /**
     * Get all complete backups, ordered from most recent to earliest.
     *
     * @return A list of backup names
     */
    public List<String> getBackupsOrdered() {
        List<String> backups = new ArrayList<>(Arrays.asList(findLocalBackups(true)));
        ScheduledBackupType.sortBackupsByTime(backups);
        return backups;
    }

    /**
     * Fetch the name of the backup directory for this task, n revisions back
     *
     * @param revision How far to go back -- 0 for latest stable version, 1 for the next oldest, etc.
     * @param type     Which backup type to use.
     * @return The name of the appropriate complete backup, if found, and null if no such backup was found
     */
    String getBackupByRevision(int revision, String type) {

        String[] backupsRaw = findLocalBackups(true);
        List<String> backups = new ArrayList<>();
        if (backupsRaw == null) {
            return null;
        }
        if ("all".equals(type)) {
            backups.addAll(Arrays.asList(backupsRaw));
            ScheduledBackupType.sortBackupsByTime(backups);
        } else {
            ScheduledBackupType backupType = ScheduledBackupType.getBackupTypes().get(type);
            for (String backup : backupsRaw) {
                if (backupType.isValidName(backup)) {
                    backups.add(backup);
                }
            }
        }
        int offset = (backups.size() - 1 - revision);
        if (revision < 0 || offset < 0 || offset >= backups.size()) {
            log.warn("[revert] fail: can't find revision={} with only {} complete backups", revision, backups.size());
            return null;
        }
        return backups.get(offset);
    }

    private void require(boolean test, String msg) throws ExecException {
        if (!test) {
            throw new ExecException(msg);
        }
    }

    private void requireNewOrEqual(Object currentValue, Object newValue, String valueName) throws IllegalArgumentException {
        if (currentValue != null && !currentValue.equals(newValue)) {
            throw new IllegalArgumentException("value mismatch for '" + valueName + "' " + newValue + " != " + currentValue);
        }
    }

    public void exec(@Nonnull CommandTaskKick kickMessage, boolean execute) throws Exception {
        // setup data directory
        jobDir = LessFiles.initDirectory(new File(minion.rootDir, id + File.separator + node + File.separator + "live"));
        File configDir = getConfigDir();
        if (!configDir.exists()) {
            LessFiles.initDirectory(configDir);
        }
        logDir = new File(jobDir, "log");
        LessFiles.initDirectory(logDir);
        replicateDone = new File(configDir, "replicate.done");
        jobRun = new File(configDir, "job.run");
        jobDone = new File(configDir, "job.done");
        logOut = new File(logDir, "log.out");
        logErr = new File(logDir, "log.err");
        jobPid = new File(configDir, "job.pid");
        jobPort = new File(jobDir, "job.port");
        jobStopped = new File(jobDir, "job.stopped");
        wasQueued = false;
        if (execute) {
            File replicateComplete = new File(getLiveDir(), "replicate.complete");
            replicateComplete.createNewFile();
            replicas = kickMessage.getReplicas();
            String jobId = kickMessage.getJobUuid();
            int jobNode = kickMessage.getJobKey().getNodeNumber();
            log.debug("[task.exec] {}", kickMessage.getJobKey());
            require(testTaskIdle(), "task is not idle");
            String jobCommand = kickMessage.getCommand();
            require(!LessStrings.isEmpty(jobCommand), "task command is missing or empty");
            // ensure we're not changing something critical on a re-spawn
            int jobNodes = kickMessage.getJobNodes();
            requireNewOrEqual(id, jobId, "Job ID");
            requireNewOrEqual(node, jobNode, "Job Node");
            requireNewOrEqual(nodeCount, jobNodes, "Job Node Count");
            // store the new values
            id = jobId;
            node = jobNode;
            nodeCount = jobNodes;
            kick = kickMessage;
            autoRetry = kick.getAutoRetry();
            // allocate type slot if applicable
            minion.sendStatusMessage(new StatusTaskBegin(minion.uuid, id, node));
            // store in jobs on first run
            if (runCount == 0) {
                log.warn("[task.exec] first time running {}", getName());
            }
            String jobConfig = kickMessage.getConfig();
            if (jobConfig != null) {
                LessFiles.write(new File(jobDir, "job.conf"), LessBytes.toBytes(jobConfig), false);
            }
            String portString = String.valueOf(0);
            // create exec command
            jobCommand = jobCommand.replace("{{jobdir}}", jobDir.getPath())
                                   .replace("{{jobid}}", jobId)
                                   .replace("{{port}}", portString)
                                   .replace("{{node}}", String.valueOf(jobNode))
                                   .replace("{{nodes}}", String.valueOf(jobNodes));
            String setEnvironmentPrefix = String.format(
                    "HYDRA_JOBDIR='%s' HYDRA_JOBID='%s' HYDRA_NODE='%s' HYDRA_NODES='%s' " +
                    "HYDRA_OWNER='%s' HYDRA_USERGROUP='%s' HYDRA_PORT='%s'",
                    jobDir.getPath(), jobId, jobNode, jobNodes,
                    kickMessage.getOwner(), kickMessage.getUserGroup(), portString);
            log.warn("[task.exec] starting {} with autoRetry={}", jobDir.getPath(), autoRetry);
            // create shell wrapper
            require(minion.deleteFiles(jobPid, jobPort, jobDone, jobStopped), "failed to delete files");
            port = null;
            String stamp = Minion.timeFormat.print(System.currentTimeMillis());
            File logOutTmp = new File(logDir, "log-" + stamp + ".out");
            File logErrTmp = new File(logDir, "log-" + stamp + ".err");
            StringBuilder bash = new StringBuilder("#!/bin/bash\n");
            bash.append("find " + logDir + " -type f -mtime +30 -exec rm {} \\;\n");
            bash.append("rm -f " + logOut + " " + logErr + "\n");
            bash.append("ln -s " + logOutTmp.getName() + " " + logOut + "\n");
            bash.append("ln -s " + logErrTmp.getName() + " " + logErr + "\n");
            bash.append("(\n");
            bash.append("cd " + jobDir + "\n");
            bash.append('(' + setEnvironmentPrefix + ' ' + jobCommand + ") &\n");
            bash.append("pid=$!\n");
            bash.append("echo ${pid} > " + jobPid.getCanonicalPath() + "\n");
            bash.append("exit=0\n");
            String taskStartHeader = String.format("Starting job/task %s/%s on host/uuid %s/%s",
                                                   jobId, jobNode, minion.myHost, minion.getUUID());
            String taskStartHeaderExtended = String.format(
                    " - job kicks %s - task runs (here/total) %s/%s - pid ${pid} - max time %s - cmd %s",
                    kick.getRunCount(), runCount, kick.getStarts(), kickMessage.getRunTime(), jobCommand);
            bash.append(Minion.echoWithDate_cmd + "\"" + taskStartHeader + taskStartHeaderExtended + "\"\n");
            bash.append("wait ${pid} || exit=$?\n");
            bash.append("echo ${exit} > " + jobDone.getCanonicalPath() + "\n");
            bash.append(Minion.echoWithDate_cmd + "Exiting task with return value: ${exit}" + "\n");
            bash.append("exit ${exit}\n");
            bash.append(") >" + logOutTmp + " 2>" + logErrTmp + " &\n");
            LessFiles.write(jobRun, LessBytes.toBytes(bash.toString()), false);
            runCount++;
            this.startTime = System.currentTimeMillis();
        }
        save();
        minion.sendHostStatus();
        // mark it active TODO: should this occur before sending updated host state?
        Minion.capacityLock.lock();
        try {
            minion.activeTaskKeys.add(getName());
        } finally {
            Minion.capacityLock.unlock();
        }
        // start watcher, which will fire it up
        workItemThread = new Thread(new RunTaskWorkItem(jobPid, jobRun, jobDone, this, execute, autoRetry));
        workItemThread.setName("RunTask-WorkItem-" + getName());
        workItemThread.start();
    }

    public void execReplicate(String rebalanceSource,
                              String rebalanceTarget,
                              boolean replicateAllBackups,
                              boolean execute,
                              boolean wasQueued) throws Exception {
        setRebalanceSource(rebalanceSource);
        setRebalanceTarget(rebalanceTarget);
        setWasQueued(wasQueued);
        if (log.isDebugEnabled()) {
            log.debug("[task.execReplicate] {}", this.getJobKey());
        }
        require(!execute || testTaskIdle(), "task is not idle");
        if (((replicas == null) || (replicas.length == 0))
            && ((failureRecoveryReplicas == null) || (failureRecoveryReplicas.length == 0))) {
            execBackup(rebalanceSource, rebalanceTarget, true);
            return;
        }
        if (execute && (ProcessUtils.findActiveRsync(id, node) != null)) {
            String msg = "Replicate failed because an existing rsync process was found for " + getName();
            log.warn("[task.execReplicate] {}", msg);
            sendEndStatus(JobTaskErrorCode.EXIT_REPLICATE_FAILURE);
            ProcessUtils.shell(Minion.echoWithDate_cmd + msg + " >> " + logErr.getCanonicalPath(), minion.rootDir);
            return;
        }
        try {
            jobDir = LessFiles.initDirectory(
                    new File(minion.rootDir, id + File.separator + node + File.separator + "live"));
            log.info("[task.execReplicate] replicating {}", jobDir.getPath());
            File configDir = getConfigDir();
            LessFiles.initDirectory(configDir);
            // create shell wrapper
            replicateSH = new File(configDir, "replicate.sh");
            replicateRun = new File(configDir, "replicate.run");
            replicateDone = new File(configDir, "replicate.done");
            replicatePid = new File(configDir, "replicate.pid");
            if (execute) {
                require(minion.deleteFiles(replicatePid, replicateDone), "failed to delete replicate config files");
                String replicateRunScript = generateRunScript(replicateSH.getCanonicalPath(), replicatePid.getCanonicalPath(), replicateDone.getCanonicalPath());
                LessFiles.write(replicateRun, LessBytes.toBytes(replicateRunScript), false);
                String replicateSHScript = generateReplicateSHScript(replicateAllBackups);
                LessFiles.write(replicateSH, LessBytes.toBytes(replicateSHScript), false);
                minion.sendStatusMessage(new StatusTaskReplicate(minion.uuid, id, node, replicateAllBackups));
                replicateStartTime = System.currentTimeMillis();
                save();
            }
            // start watcher
            Runnable workItem = new ReplicateWorkItem(replicatePid, replicateRun, replicateDone, this,
                                                      rebalanceSource, rebalanceTarget, execute);
            workItemThread = new Thread(workItem, "Replicate-WorkItem-" + getName());
            workItemThread.start();
        } catch (Exception ex) {
            sendEndStatus(JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR);
            throw ex;
        }
    }

    public void execBackup(String rebalanceSource, String rebalanceTarget, boolean execute) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("[task.execBackup] {}", this.getJobKey());
        }
        require(!execute || testTaskIdle(), "task is not idle");
        try {
            log.info("[task.execBackup] backing up {}", jobDir.getPath());
            File configDir = getConfigDir();
            LessFiles.initDirectory(configDir);
            backupSH = new File(configDir, "backup.sh");
            backupRun = new File(configDir, "backup.run");
            backupDone = new File(configDir, "backup.done");
            backupPid = new File(configDir, "backup.pid");
            if (execute) {
                require(minion.deleteFiles(backupPid, backupDone), "failed to delete backup config files");
                String backupSHScript = generateBackupSHScript(replicas);
                LessFiles.write(backupSH, LessBytes.toBytes(backupSHScript), false);
                String backupRunScript = generateRunScript(backupSH.getCanonicalPath(), backupPid.getCanonicalPath(), backupDone.getCanonicalPath());
                LessFiles.write(backupRun, LessBytes.toBytes(backupRunScript), false);
                minion.sendStatusMessage(new StatusTaskBackup(minion.uuid, id, node));
                backupStartTime = System.currentTimeMillis();
                save();
            }
            workItemThread = new Thread(new BackupWorkItem(backupPid, backupRun, backupDone, this, rebalanceSource, rebalanceTarget, execute));
            workItemThread.setName("Backup-WorkItem-" + getName());
            workItemThread.start();
        } catch (Exception ex) {
            sendEndStatus(JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR);
            throw ex;
        }
    }

    private String makeRetryDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("retries=" + Minion.copyRetryLimit + "\n");
        sb.append("retryDelaySeconds=" + Minion.copyRetryDelaySeconds + "\n");
        sb.append("function retry {\n" +
                "try=0; cmd=\"$@\"\n" +
                "until [ $try -ge $retries ]; do\n" +
                "\tif [ \"$try\" -ge \"1\" ]; then echo starting retry $try; sleep $retryDelaySeconds; fi\n" +
                "\ttry=$((try+1)); eval $cmd; exitCode=$?\n" +
                "\tif [ \"$exitCode\" == \"0\" ]; then return $exitCode; fi\n" +
                "done\n" +
                "echo \"Command failed after $retries retries: $cmd\"; exit $exitCode\n" +
                "}\n");
        return sb.toString();
    }

    private String generateReplicateSHScript(boolean replicateAllBackups) throws IOException {
        logDir = new File(jobDir, "log");
        LessFiles.initDirectory(logDir);
        StringBuilder bash = new StringBuilder("#!/bin/bash\n");
        bash.append(makeRetryDefinition());
        bash.append(Minion.echoWithDate_cmd + "Deleting environment lock files in preparation for replication\n");
        bash.append("find " + jobDir.getCanonicalPath() + " -name je.lck -print -exec rm {} \\;\n");
        bash.append("find " + jobDir.getCanonicalPath() + " -name je.info.0 -print -exec rm {} \\;\n");
        appendReplicas(bash, failureRecoveryReplicas, true); // Add commands for any the failure-recovery replicas that definitely need full rsyncs
        appendReplicas(bash, replicas, replicateAllBackups); // Add commands for the existing replicas
        bash.append(Minion.echoWithDate_cmd + "Finished replicating successfully\n");
        return bash.toString();
    }

    private void appendReplicas(StringBuilder bash, ReplicaTarget[] replicas, boolean replicateAllBackups) throws IOException {
        if (replicas == null) {
            return;
        }
        for (ReplicaTarget replica : replicas) {
            if (replica.getHostUuid() == null || replica.getHostUuid().equals(minion.uuid)) {
                return;
            }
            List<String> replicateCommands = assembleReplicateCommandAndInformSpawn(replica, replicateAllBackups);
            if (replicateCommands == null || replicateCommands.isEmpty()) {
                return;
            }
            String action = "replicating to " + replica.getHost() + " uuid=" + replica.getHostUuid();
            appendCommandsWithStartFinishMessages(bash, action, replicateCommands, minion.replicateCommandDelaySeconds);
        }
    }

    private String generateRunScript(String shName, String pidPath, String donePath) throws IOException {
        if (logOut == null || logErr == null) {
            File logRoot = new File(jobDir, "log");
            logOut = new File(logRoot, "log.out");
            logErr = new File(logRoot, "log.err");
        }
        StringBuilder bash = new StringBuilder("#!/bin/bash\n");
        bash.append("(\n");
        bash.append("\t cd " + jobDir.getCanonicalPath() + "\n");
        bash.append("\t (bash " + shName + ") &\n");
        bash.append("\t pid=$!\n");
        bash.append("\t echo ${pid} > " + pidPath + "\n");
        bash.append("\t exit=0\n");
        bash.append("\t wait ${pid} || exit=$?\n");
        bash.append("\t echo ${exit} > " + donePath + "\n");
        bash.append("\t exit ${exit};\n");
        bash.append(") >> " + logOut.getCanonicalPath() + " 2>> " + logErr.getCanonicalPath() + " &");
        return bash.toString();
    }

    private String generateBackupSHScript(ReplicaTarget[] replicas) throws IOException {
        logDir = new File(jobDir, "log");
        LessFiles.initDirectory(logDir);
        StringBuilder bash = new StringBuilder("#!/bin/bash\n");
        bash.append("cd " + jobDir.getCanonicalPath() + "\n");
        bash.append(makeRetryDefinition());
        List<String> symlinkCommands = new ArrayList<>();
        List<String> deleteCommands = new ArrayList<>();
        long now = System.currentTimeMillis();
        List<String> localBackupCommands = assembleBackupCommandsForHost(true, null, symlinkCommands, deleteCommands, now);
        appendCommandsWithStartFinishMessages(bash, "updating local backups", localBackupCommands, minion.backupCommandDelaySeconds);
        if (replicas != null) {
            for (ReplicaTarget replica : replicas) {
                if (replica.getHostUuid() == null || replica.getHostUuid().equals(minion.uuid)) {
                    continue;
                }
                String action = "updating backups on " + replica.getHost() + " uuid=" + replica.getHostUuid();
                List<String> remoteBackupCommands = assembleBackupCommandsForHost(false, replica, symlinkCommands, deleteCommands, now);
                appendCommandsWithStartFinishMessages(bash, action, remoteBackupCommands, minion.backupCommandDelaySeconds);
            }
        }
        appendCommandsWithStartFinishMessages(bash, "updating symlinks", symlinkCommands, minion.backupCommandDelaySeconds);
        appendCommandsWithStartFinishMessages(bash, "deleting old backups", deleteCommands, minion.backupCommandDelaySeconds);
        bash.append(Minion.echoWithDate_cmd + "Finished backing up successfully\n");
        return bash.toString();
    }

    private void appendCommandsWithStartFinishMessages(StringBuilder builder, String action, List<String> commands, int delaySeconds) {
        builder.append(Minion.echoWithDate_cmd + " Started " + action + " \n");
        for (String cmd : commands) {
            if (delaySeconds > 0) {
                builder.append("sleep " + delaySeconds + " && \\\n");
            }
            builder.append(cmd + " && \\\n");
        }
        builder.append(Minion.echoWithDate_cmd + " Finished " + action + " \n");
    }

    /**
     * Suppose we have received a message to begin running a task / replicating / backing up.
     * If we're already doing one of these, reject the received instruction and re-send an event describing what we're doing.
     *
     * @return true only if the task was really idle.
     */
    private boolean testTaskIdle() {
        if (isRunning()) {
            minion.sendStatusMessage(new StatusTaskBegin(minion.uuid, id, node));
            return false;
        } else if (isReplicating()) {
            minion.sendStatusMessage(new StatusTaskReplicate(minion.uuid, id, node, false));
            return false;
        } else if (isBackingUp()) {
            minion.sendStatusMessage(new StatusTaskBackup(minion.uuid, id, node));
            return false;
        } else if (workItemThread != null) {
            log.warn("clearing workItem for idle task {}", getName());
            workItemThread.interrupt();
            workItemThread = null;
        }
        return true;
    }

    boolean isProcessRunning(File pidFile) {
        Integer pid = ProcessUtils.getPID(pidFile);
        return pid != null && ProcessUtils.activeProcessExistsWithPid(pid, minion.rootDir);
    }

    protected void createDoneFileIfNoProcessRunning(File pidFile, File doneFile) {
        if (doneFile == null || pidFile == null || doneFile.exists()) {
            return;
        }
        boolean success = false;
        try {
            Integer pid = ProcessUtils.getPID(pidFile);
            if (pid == null || !ProcessUtils.activeProcessExistsWithPid(pid, minion.rootDir)) {
                success = doneFile.exists() || doneFile.createNewFile();
            } else {
                success = true; // Process exists, nothing to do.
            }
        } catch (IOException io) {
            success = false;
            log.warn("[task.state.check] exception when creating done file", io);
        }
        if (!success) {
            log.warn("[task.state.check] failed to create done file for task {} path {}", getName(), doneFile);
        }
    }

    public String getName() {
        return id + "/" + node;
    }

    public File getJobDir() {
        return jobDir;
    }

    public Integer getPort() {
        try {
            if (port == null && jobPort.exists())// && jobPort.lastModified() >= jobRun.lastModified())
            {
                port = Integer.parseInt(LessBytes.toString(LessFiles.read(jobPort)));
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        return port;
    }

    // TODO hookup to a job clean cmd at some point (for testing mostly)
    public boolean deleteData() {
        return false;
    }

    public boolean isRunning() {
        if (jobDone == null) {
            return false;
        }
        // no checking for process here since this doesn't seem to be broken like the others
        return this.startTime > 0 && !jobDone.exists();
    }

    public boolean isReplicating() {
        if (replicateDone == null) {
            return false;
        }
        return !isRunning() && replicateStartTime > 0 && !replicateDone.exists() && isProcessRunning(replicatePid);
    }

    public boolean isBackingUp() {
        if (backupDone == null) {
            return false;
        }
        return !isRunning() && !isReplicating() && backupStartTime > 0 && !backupDone.exists() && isProcessRunning(backupPid);
    }

    public File[] getActivePidFiles() {
        if (isRunning()) {
            return new File[]{jobPid};
        } else if (isReplicating()) {
            return new File[]{replicatePid};
        } else if (isBackingUp()) {
            return new File[]{backupPid};
        } else {
            return null;
        }
    }

    public boolean stopWait(boolean kill) {
        File[] activePidFiles = getActivePidFiles();
        Integer rsync = null;
        if (isReplicating()) {
            rsync = ProcessUtils.findActiveRsync(id, node);
        }
        boolean success = activePidFiles != null && stopWait(activePidFiles, kill);
        if (rsync != null) {
            // Need to kill the rsync after the replicate script to avoid doing a retry
            ProcessUtils.shell("kill -9 " + rsync, minion.rootDir);
        }
        return success;
    }

    public boolean stopWait(File[] pidFiles, boolean kill) {
        boolean result = true;
        boolean isRunning = isRunning();
        try {
            if (kill) {
                resetStartTime();
                log.warn("[stopWait] creating done files for {} if they do not exist", getName());
                if (!jobDone.getParentFile().exists()) {
                    log.warn("The directory {} does not exist.", jobDone.getParent());
                } else {
                    createDoneFileIfNoProcessRunning(jobPid, jobDone);
                    createDoneFileIfNoProcessRunning(replicatePid, replicateDone);
                    createDoneFileIfNoProcessRunning(backupPid, backupDone);
                }
            }
            for (File pidFile : pidFiles) {
                Integer pid = ProcessUtils.getPID(pidFile);
                if (pid == null) {
                    log.warn("{}Wait failed with null pid for {}", kill ? "stop" : "kill", getName());
                    result = false;
                } else {
                    if (pid.equals(minion.minionPid)) {
                        log.warn("[minion.kill] tried to kill my own process. pid: {}", pid);
                        result = false;
                    }
                    String cmd = ProcessUtils.getCmdLine(pid);
                    if (cmd == null) {
                        log.warn("[minion.kill] unable to read cmdline, so it seems unlikely the process is running, ret false");
                        result = false;
                    } else {
                        log.warn("[minion.kill] about to kill pid {} with cmd line: {}", pid, cmd);
                        if (cmd.contains(" minion") || cmd.contains(" mss") || cmd.contains(" mqworker")) {
                            log.warn("It looked like we are trying to kill an Important Process (TM), returning false instead");
                            result = false;
                        }
                    }
                    if (isRunning) {
                        jobStopped = new File(jobDir, "job.stopped");
                        if (!jobStopped.createNewFile()) {
                            log.warn("Failed to create job.stopped file for stopped job {}", getName());
                        }
                    }
                    if (kill) {
                        log.warn("[minion.kill] killing pid:{} hard", pid);
                        result &= ProcessUtils.shell("kill -9 " + pid, minion.rootDir) >= 0;
                    } else {
                        log.warn("[minion.kill] killing pid:{} nice", pid);
                        result &= ProcessUtils.shell("kill " + pid, minion.rootDir) >= 0;
                    }
                }
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        return result;
    }

    private void resetStartTime() {
        if (isRunning()) {
            startTime = 0;
        } else if (isReplicating()) {
            replicateStartTime = 0;
        } else if (isBackingUp()) {
            backupStartTime = 0;
        }
        minion.writeState();
    }

    public File getLiveDir() {
        return new File(taskRoot, "live");
    }

    public File getConfigDir() {
        return new File(taskRoot, "config");
    }

    public String profile() {
        File profile = new File(jobDir, "job.profile");
        if (profile.exists()) {
            try {
                return LessBytes.toString(LessFiles.read(profile));
            } catch (IOException e) {
                log.warn("IO problem while trying to read job.profile", e);
            }
        }
        return "";
    }

    public void setRuntime(long runTime) {
        this.runTime = runTime;
    }

    public void setReplicas(ReplicaTarget[] replicas) {
        this.replicas = replicas;
    }

    public void setFailureRecoveryReplicas(ReplicaTarget[] replicas) {
        this.failureRecoveryReplicas = replicas;
    }

    public ReplicaTarget[] getFailureRecoveryReplicas() {
        return failureRecoveryReplicas;
    }

    public ReplicaTarget[] getReplicas() {
        return replicas;
    }

    public boolean wasStopped() {
        if (jobStopped == null) {
            jobStopped = new File(jobDir, "job.stopped");
        }
        return jobStopped.exists();
    }

    public String getRebalanceSource() {
        return rebalanceSource;
    }

    public void setRebalanceSource(String rebalanceSource) {
        this.rebalanceSource = rebalanceSource;
    }

    public boolean wasQueued() {
        return wasQueued;
    }

    public void setWasQueued(boolean wasQueued) {
        this.wasQueued = wasQueued;
    }

    public String getRebalanceTarget() {
        return rebalanceTarget;
    }

    public void setRebalanceTarget(String rebalanceTarget) {
        this.rebalanceTarget = rebalanceTarget;
    }

    @Override
    public String toString() {
        return "JobTask{" +
                "id='" + id + '\'' +
                ", node=" + node +
                ", jobDir=" + jobDir +
                '}';
    }

    /**
     * Attempt to identify the task's last end status from the file system
      * @return An integer representing the task's last exit code
     */
    public int findLastJobStatus() {
        if (jobDone != null && jobDone.exists()) {
            try {
                String jobDoneString = LessBytes.toString(LessFiles.read(jobDone));
                if (jobDoneString == null || jobDoneString.isEmpty()) {
                    return 0;
                }
                return Integer.parseInt(jobDoneString.trim());
            } catch (IOException e) {
                return JobTaskErrorCode.EXIT_SCRIPT_EXEC_ERROR;
            }
        }
        return 0;
    }
}

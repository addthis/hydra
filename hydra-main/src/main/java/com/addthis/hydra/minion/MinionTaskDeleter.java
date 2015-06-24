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

import java.io.File;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.SimpleExec;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.job.backup.BackupToDelete;
import com.addthis.hydra.job.backup.ScheduledBackupType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MinionTaskDeleter implements Codable {

    private static final Logger log = LoggerFactory.getLogger(MinionTaskDeleter.class);

    @FieldConfig(codable = true)
    private final ConcurrentSkipListSet<String> tasksToDelete;
    @FieldConfig(codable = true)
    private final ConcurrentSkipListSet<BackupToDelete> backupsToDelete;

    private static final Map<String, ScheduledBackupType> backupTypesByDesc = ScheduledBackupType.getBackupTypes();
    private static final Map<ScheduledBackupType, Long> protectedBackupTypes = ScheduledBackupType.getProtectedBackupTypes();
    private static final long deleteCheckFrequency = Parameter.longValue("delete.check.frequency", 60000L);
    /* If disk is above x%, delete backups immediately */
    private static final double deleteImmediatelyDiskThreshold = Double.parseDouble(Parameter.value("delete.immediately.disk.threshold", ".9"));

    private Thread deletionThread;

    public MinionTaskDeleter() {
        this.tasksToDelete = new ConcurrentSkipListSet<>();
        this.backupsToDelete = new ConcurrentSkipListSet<>();
    }

    /**
     * Tell MinionDeleter to add a path for deletion
     *
     * @param taskPath The path to be deleted
     */
    public void submitPathToDelete(String taskPath) {
        synchronized (tasksToDelete) {
            tasksToDelete.add(taskPath);
        }
    }

    /**
     * Tell MinionDeleter to add a backup for deletion
     *
     * @param backupPath The path to the backup
     * @param type       The type of backup (e.g. gold)
     */
    public void submitBackupToDelete(String backupPath, ScheduledBackupType type) {
        synchronized (backupsToDelete) {
            backupsToDelete.add(new BackupToDelete(backupPath, type.getDescription()));
        }
    }

    /**
     * Start a thread that will periodically check to see if any stored items can be deleted.
     */
    public synchronized void startDeletionThread() {
        deletionThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(deleteCheckFrequency);
                    deleteStoredItems();
                } catch (InterruptedException ex) {
                    break;
                } catch (Exception e) {
                    log.warn("Exception during MinionTaskDeleter execution: ", e);
                }
            }

        });
        deletionThread.setName("minion-deleter-thread");
        deletionThread.setDaemon(true);
        deletionThread.start();
    }

    /**
     * Stop the thread that deletes minions periodically (primarily when a minion is shutting down.)
     */
    public synchronized void stopDeletionThread() {
        if (deletionThread != null) {
            deletionThread.interrupt();
        }
    }


    /**
     * Delete the stored tasks and backups if it is appropriate to do so.
     */
    public synchronized void deleteStoredItems() {
        List<String> taskSnapshot;
        synchronized (tasksToDelete) {
            // Make a snapshot of tasks, so that we don't block task additions on lengthy delete operations
            taskSnapshot = new ArrayList<>(tasksToDelete);
        }
        for (String task : taskSnapshot) {
            deleteTask(task);
        }
        synchronized (tasksToDelete) {
            // Remove all tasks that were in our initial snapshot
            tasksToDelete.removeAll(taskSnapshot);
        }
        List<BackupToDelete> backupSnapshot;
        synchronized (backupsToDelete) {
            backupSnapshot = new ArrayList<>(backupsToDelete);
        }
        Iterator<BackupToDelete> backupIter = backupSnapshot.iterator();
        while (backupIter.hasNext()) {
            BackupToDelete backup = backupIter.next();
            // If we don't delete a backup because it's not old enough yet, remove it from the snapshot so we don't remove it from the backupsToDelete list
            if (!executeDelete(backup)) {
                backupIter.remove();
            }
        }
        synchronized (backupsToDelete) {
            // Remove all backups that deleted successfully from backupsToDelete
            backupsToDelete.removeAll(backupSnapshot);
        }
    }

    /**
     * Visit a directory and delete all files/directories except those that are protected backup types
     *
     * @param taskPath The path to be deleted
     */
    private void deleteTask(String taskPath) {
        File taskDirectory;
        File[] files;
        if (taskPath == null || !(taskDirectory = new File(taskPath)).exists() || (files = taskDirectory.listFiles()) == null) {
            return;
        }
        for (File file : files) {
            String fileName = file.getName();
            if (!fileName.startsWith(ScheduledBackupType.getBackupPrefix())) {
                deleteFile(file);
            } else {
                boolean wasProtected = false;
                for (ScheduledBackupType protectedType : protectedBackupTypes.keySet()) {
                    if (protectedType.isValidName(fileName)) {
                        submitBackupToDelete(file.getAbsolutePath(), protectedType);
                        wasProtected = true;
                        break;
                    }
                }
                if (!wasProtected) {
                    deleteFile(file);
                }
            }
        }
        deleteIfEmpty(taskDirectory);
        deleteIfEmpty(taskDirectory.getParentFile());
    }

    /**
     * Attempt to execute a delete on the specified backup.
     *
     * @param backupToDelete The backup object to delete
     * @return True if the backup was either invalid or was successfully deleted
     */
    public boolean executeDelete(BackupToDelete backupToDelete) {
        String backupType = backupToDelete != null ? backupToDelete.getBackupType() : null;
        String backupPath = backupToDelete != null ? backupToDelete.getBackupPath() : null;
        if (backupType == null || !backupTypesByDesc.containsKey(backupType)) {
            log.warn("Tried to delete invalid backup " + this.toString());
            return true;
        }
        File backupFile = new File(backupPath);
        if (!backupFile.exists()) {
            log.warn("File was already deleted: " + backupPath);
            return true;
        }
        String backupName = backupFile.getName();
        ScheduledBackupType type = backupTypesByDesc.get(backupType);
        if (!type.isValidName(backupName)) {
            log.warn("Backup did not have valid name: " + backupName);
            return true;
        }
        if (!protectedBackupTypes.containsKey(type)) {
            deleteFile(backupFile);
            return true;
        }
        if (diskIsFull(backupFile) || shouldDeleteBackup(backupName, type)) {
            File taskDir = backupFile.getParentFile();
            File jobDir = taskDir.getParentFile();
            deleteFile(backupFile);
            deleteIfEmpty(taskDir);
            deleteIfEmpty(jobDir);
            return true;
        }
        return false;
    }

    /**
     * Delete a directory if it is empty (for example, delete the task directory when we remove the last backup)
     *
     * @param file The directory to delete.
     */
    private static void deleteIfEmpty(File file) {
        if (file.isDirectory() && file.list().length == 0) {
            try {
                // Use rmdir to ensure failure if files are unexpectedly created
                new SimpleExec("rmdir " + file.getAbsolutePath()).join();
            } catch (Exception e) {
                log.warn("deleteIfEmpty failed on " + file + ": " + e);
            }
        }
    }

    /**
     * Is it okay to delete the given backup, given its age and the current time?
     *
     * @param backupName The backup to delete
     * @param type       The type of backup
     * @return True if the backup should be deleted
     */
    public static boolean shouldDeleteBackup(String backupName, ScheduledBackupType type) {
        if (type.isValidName(backupName) && protectedBackupTypes.containsKey(type)) {
            long age;
            try {
                age = System.currentTimeMillis() - type.parseDateFromName(backupName).getTime();
                return age > protectedBackupTypes.get(type);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return true;
    }

    private static boolean diskIsFull(File dir) {
        double avail = (double) (dir.getFreeSpace()) / dir.getTotalSpace();
        return avail < 1 - deleteImmediatelyDiskThreshold;
    }

    /**
     * Simple wrapper around SimpleExec to run rm -rc on a faile
     *
     * @param file The file to delete
     * @return True if the file was deleted successfully
     */
    private static boolean deleteFile(File file) {
        try {
            SimpleExec exec = new SimpleExec("rm -rf " + file.getAbsolutePath()).join();
            return exec.exitCode() == 0;
        } catch (Exception e) {
            log.warn("Failed to delete file at path " + file.getAbsolutePath());
            return false;
        }
    }

    /**
     * Get a copy of the tasksToDelete. Necessary for serialization.
     *
     * @return The set of task paths
     */
    public Set<String> getTasksToDelete() {
        synchronized (tasksToDelete) {
            return new HashSet<>(tasksToDelete);
        }
    }

    /**
     * Get a copy of the backupsToDelete. Necessary for serialization
     *
     * @return The set of backups
     */
    public Set<BackupToDelete> getBackupsToDelete() {
        synchronized (backupsToDelete) {
            return new HashSet<>(backupsToDelete);
        }
    }
}

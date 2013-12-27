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
package com.addthis.hydra.job.backup;

/**
 * A class in charge of tracking a particular backup event, including the machines and directories involved.
 */
public class BackupEvent {

    private final boolean local; // Was the backup local?
    private final String userAT; // The ssh user string, if applicable
    private String baseDir; // Base (remote) directory for this backup
    private String createdDir; // The dir created by this backup. Starts out null, and gets set if the backup succeeds.
    private final ScheduledBackupType type; // The type of backup -- daily, weekly, etc.

    public BackupEvent(boolean local, String userAT, ScheduledBackupType type, String baseDir) {
        this.local = local;
        this.userAT = userAT;
        this.baseDir = baseDir;
        this.createdDir = null;
        this.type = type;
    }

    public boolean isLocal() {
        return local;
    }

    public String getUserAT() {
        return userAT;
    }

    public String getCreatedDir() {
        return createdDir;
    }

    public String getBackupName() {
        if (createdDir != null) {
            String[] dirs = createdDir.split("/");
            return dirs[dirs.length - 1];
        }
        return null;
    }

    public void setCreatedDir(String createdDir) {
        this.createdDir = createdDir;
    }

    public String getBaseDir() {
        return this.baseDir;
    }


    public ScheduledBackupType getType() {
        return type;
    }

}

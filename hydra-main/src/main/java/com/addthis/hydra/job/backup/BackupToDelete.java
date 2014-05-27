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

import com.addthis.codec.Codec;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class BackupToDelete implements Codec.Codable, Comparable {

    private static Logger log = LoggerFactory.getLogger(BackupToDelete.class);
    @Codec.Set(codable = true)
    private String backupPath;
    @Codec.Set(codable = true)
    private String backupType;

    public BackupToDelete() {
    }

    public BackupToDelete(String backupPath, String backupType) {
        this.backupPath = backupPath;
        this.backupType = backupType;
    }

    public String getBackupPath() {
        return backupPath;
    }

    public String getBackupType() {
        return backupType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BackupToDelete that = (BackupToDelete) o;

        if (backupPath != null ? !backupPath.equals(that.backupPath) : that.backupPath != null) return false;
        if (backupType != null ? !backupType.equals(that.backupType) : that.backupType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = backupPath != null ? backupPath.hashCode() : 0;
        result = 31 * result + (backupType != null ? backupType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BackupToDelete{" +
               "backupPath='" + backupPath + '\'' +
               ", backupType='" + backupType + '\'' +
               '}';
    }

    @Override
    public int compareTo(Object o) {
        return o != null ? Integer.compare(this.hashCode(), o.hashCode()) : 0;
    }
}

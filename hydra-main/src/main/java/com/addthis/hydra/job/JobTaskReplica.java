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

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * marker data for a replica copy of data
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JobTaskReplica implements Codable {

    @FieldConfig(codable = true)
    private String hostUuid;
    @FieldConfig(codable = true)
    private String jobUuid;
    @FieldConfig(codable = true)
    private int version; // equates to runCount in task
    @FieldConfig(codable = true)
    private long updated;

    public JobTaskReplica() {
    }

    public JobTaskReplica(String hostUuid, String jobUuid, int version, long updated) {
        this.hostUuid = hostUuid;
        this.jobUuid = jobUuid;
        this.version = version;
        this.updated = updated;
    }

    public String getHostUUID() {
        return hostUuid;
    }

    public void setHostUUID(String uuid) {
        hostUuid = uuid;
    }

    public String getJobUUID() {
        return jobUuid;
    }

    public void setJobUUID(String uuid) {
        jobUuid = uuid;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }


    public long getLastUpdate() {
        return updated;
    }

    public void setLastUpdate(long updated) {
        this.updated = updated;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JobTaskReplica other = (JobTaskReplica) obj;
        if ((this.hostUuid == null) ? (other.hostUuid != null) : !this.hostUuid.equals(other.hostUuid)) {
            return false;
        }
        if ((this.jobUuid == null) ? (other.jobUuid != null) : !this.jobUuid.equals(other.jobUuid)) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        if (this.updated != other.updated) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JobTaskReplica{" +
               "hostUuid='" + hostUuid + '\'' +
               ", jobUuid='" + jobUuid + '\'' +
               ", version=" + version +
               '}';
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = 31 * hash + (this.hostUuid != null ? this.hostUuid.hashCode() : 0);
        hash = 31 * hash + (this.jobUuid != null ? this.jobUuid.hashCode() : 0);
        hash = 31 * hash + this.version;
        hash = 31 * hash + (int) (this.updated ^ (this.updated >>> 32));
        return hash;
    }


}

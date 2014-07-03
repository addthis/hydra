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
package com.addthis.hydra.job.mq;

import java.io.Serializable;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

import org.codehaus.jackson.annotate.JsonIgnore;

public class JobKey implements Codable, Serializable {

    private static final long serialVersionUID = -7403399606713468228L;

    @FieldConfig(codable = true)
    private String jobUuid;
    @FieldConfig(codable = true)
    private Integer nodeNumber;

    public JobKey() {
    }

    public JobKey(String jobUuid, Integer nodeNumber) {
        this.jobUuid = jobUuid;
        this.nodeNumber = nodeNumber;
    }

    public boolean matches(JobKey key) {
        if (key.jobUuid == null || jobUuid == null || !key.jobUuid.equals(jobUuid)) {
            return false;
        }
        return key.nodeNumber == null || nodeNumber == null || key.nodeNumber.equals(nodeNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobKey other = (JobKey) o;
        return this.matches(other);
    }

    @Override
    public int hashCode() {
        int result = jobUuid != null ? jobUuid.hashCode() : 0;
        result = 31 * result + (nodeNumber != null ? nodeNumber.hashCode() : 0);
        return result;
    }

    public String getJobUuid() {
        return jobUuid;
    }

    public Integer getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(Integer nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public void setJobUuid(String jobUuid) {
        this.jobUuid = jobUuid;
    }

    @JsonIgnore
    public String getJobKey() {
        return jobUuid + "/" + nodeNumber;
    }

    @Override
    public String toString() {
        return getJobKey();
    }
}

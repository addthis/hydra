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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
public abstract class AbstractJobMessage implements JobMessage {

    @JsonProperty private String hostUuid;
    @JsonProperty private String jobUuid;
    @JsonProperty private Integer nodeId;

    @JsonCreator
    protected AbstractJobMessage() {}

    public AbstractJobMessage(String hostUuid, String job, Integer node) {
        this.hostUuid = hostUuid;
        this.jobUuid = job;
        this.nodeId = node;
    }

    @Override
    public String getHostUuid() {
        return hostUuid;
    }

    @Override
    public String getJobUuid() {
        return jobUuid;
    }

    @Override
    public Integer getNodeID() {
        return nodeId;
    }

    @Override
    public JobKey getJobKey() {
        return new JobKey(jobUuid, nodeId);
    }
}

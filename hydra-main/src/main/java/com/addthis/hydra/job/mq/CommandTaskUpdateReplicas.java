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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskUpdateReplicas.class)
public class CommandTaskUpdateReplicas extends AbstractJobMessage {

    @JsonProperty private Set<String> failedHosts;

    @JsonProperty private List<ReplicaTarget> newReplicaHosts;

    @JsonCreator
    private CommandTaskUpdateReplicas() {
        super();
    }

    public CommandTaskUpdateReplicas(String hostUuid, String job, Integer node, Set<String> failedHosts, List<ReplicaTarget> newReplicaHosts) {
        super(hostUuid, job, node);
        this.failedHosts = failedHosts;
        this.newReplicaHosts = newReplicaHosts;
    }

    public Set<String> getFailedHosts() {
        return failedHosts != null ? failedHosts : new HashSet<>();
    }

    public List<ReplicaTarget> getNewReplicaHosts() {
        return newReplicaHosts != null ? newReplicaHosts : new ArrayList<>();
    }
}

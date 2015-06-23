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

import com.addthis.codec.annotations.FieldConfig;

public class CommandTaskUpdateReplicas extends AbstractJobMessage {

    private static final long serialVersionUID = -293871238710982L;

    @FieldConfig(codable = true)
    private final Set<String> failedHosts;

    @FieldConfig(codable = true)
    private final List<ReplicaTarget> newReplicaHosts;

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

    @Override
    public TYPE getMessageType() {
        return TYPE.CMD_TASK_UPDATE_REPLICAS;
    }
}

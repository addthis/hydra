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

import java.util.Arrays;

import com.addthis.codec.Codec;

public class CommandTaskReplicate extends AbstractJobMessage {

    private static final long serialVersionUID = 3232052848594886109L;

    public CommandTaskReplicate(String hostUuid, String job, int node, ReplicaTarget replicas[], String jobCommand, String choreWatcherKey, boolean force) {
        super(hostUuid, job, node);
        this.replicas = replicas;
        this.choreWatcherKey = choreWatcherKey;
        this.force = force;
        this.jobCommand = jobCommand;
    }

    @Codec.Set(codable = true)
    private ReplicaTarget[] replicas;
    @Codec.Set(codable = true)
    private String choreWatcherKey;
    // not all jobs replicate on every execution, if this value is true it will force the replica to occur
    @Codec.Set(codable = true)
    private boolean force;

    @Codec.Set(codable = true)
    private String jobCommand;

    public ReplicaTarget[] getReplicas() {
        return replicas;
    }

    public String getChoreWatcherKey() {
        return choreWatcherKey;
    }

    @Override
    public TYPE getMessageType() {
        return TYPE.CMD_TASK_REPLICATE;
    }

    public String getJobCommand() {
        return jobCommand;
    }

    public boolean isForce() {
        return force;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommandTaskReplicate that = (CommandTaskReplicate) o;

        if (!Arrays.equals(replicas, that.replicas)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return replicas != null ? Arrays.hashCode(replicas) : 0;
    }
}

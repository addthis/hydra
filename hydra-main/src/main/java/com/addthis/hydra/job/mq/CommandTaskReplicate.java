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

import javax.annotation.Nullable;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskReplicate.class)
public class CommandTaskReplicate extends AbstractJobMessage {

    @JsonProperty
    private ReplicaTarget[] replicas;
    @JsonProperty
    private String choreWatcherKey;
    // not all jobs replicate on every execution, if this value is true it will force the replica to occur
    @JsonProperty
    private boolean force;
    /* Whether the task was queued when the replication was done. If so, it will be re-queued on completion */
    @JsonProperty
    private boolean wasQueued;
    @JsonProperty
    private String jobCommand;
    /* For rebalances, these are the hosts that are gaining/losing a replica */
    @JsonProperty
    private String rebalanceSource;
    @JsonProperty
    private String rebalanceTarget;

    @JsonCreator
    private CommandTaskReplicate() {
        super();
    }

    public CommandTaskReplicate(String hostUuid,
                                String job,
                                int node,
                                @Nullable ReplicaTarget[] replicas,
                                @Nullable String jobCommand,
                                @Nullable String choreWatcherKey,
                                boolean force,
                                boolean wasQueued) {
        super(hostUuid, job, node);
        this.replicas = replicas;
        this.choreWatcherKey = choreWatcherKey;
        this.force = force;
        this.jobCommand = jobCommand;
        this.wasQueued = wasQueued;
    }

    public ReplicaTarget[] getReplicas() {
        return replicas;
    }

    public String getChoreWatcherKey() {
        return choreWatcherKey;
    }

    public String getJobCommand() {
        return jobCommand;
    }

    public boolean isForce() {
        return force;
    }

    public String getRebalanceSource() {
        return rebalanceSource;
    }

    public void setRebalanceSource(String rebalanceSource) {
        this.rebalanceSource = rebalanceSource;
    }

    public String getRebalanceTarget() {
        return rebalanceTarget;
    }

    public void setRebalanceTarget(String rebalanceTarget) {
        this.rebalanceTarget = rebalanceTarget;
    }

    public boolean wasQueued() {
        return wasQueued;
    }

    public void setWasQueued(boolean wasQueued) {
        this.wasQueued = wasQueued;
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

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskRevert.class)
public class CommandTaskRevert extends AbstractJobMessage {

    @JsonProperty private int revision;
    @JsonProperty private long time;
    @JsonProperty private String backupType;
    @JsonProperty private ReplicaTarget[] replicas;
    @JsonProperty private boolean skipMove;

    @JsonCreator
    private CommandTaskRevert() {
        super();
    }

    public CommandTaskRevert(String host, String job, Integer node, String backupType, int rev, long time, @Nullable
    ReplicaTarget[] replicas, boolean skipMove) {
        super(host, job, node);
        this.revision = rev;
        this.time = time;
        this.backupType = backupType;
        this.replicas = replicas;
        this.skipMove = skipMove;
    }

    /*
     * return revisions back to pull. 0 == most recent.
     */
    public int getRevision() {
        return revision;
    }

    /*
     * return time of backup to pull
     */
    public long getTime() {
        return time;
    }

    public String getBackupType() {
        return backupType;
    }

    public ReplicaTarget[] getReplicas() {
        return replicas;
    }


    /* Whether to skip the mv gold live step and just rerun the replicate/backup */
    public boolean getSkipMove() {
        return skipMove;
    }
}

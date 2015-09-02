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
package com.addthis.hydra.job.spawn;

import com.addthis.hydra.job.mq.JobKey;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A class representing an item that can sit on Spawn's queue. */
@JsonIgnoreProperties("ignoreQuiesce")
public class SpawnQueueItem extends JobKey {

    public final int priority;
    public final long creationTime;

    @JsonCreator
    private SpawnQueueItem(@JsonProperty("jobUuid") String jobUuid,
                           @JsonProperty("nodeNumber") Integer nodeNumber,
                           @JsonProperty("priority") int priority,
                           @JsonProperty("creationTime") long creationTime) {
        super(jobUuid, nodeNumber);
        this.priority = priority;
        this.creationTime = creationTime;
    }

    public SpawnQueueItem(JobKey key, int priority) {
        this(key.getJobUuid(), key.getNodeNumber(), priority, System.currentTimeMillis());
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                      .add("jobKey", getJobKey())
                      .add("priority", priority)
                      .add("creationTime", creationTime)
                      .toString();
    }
}

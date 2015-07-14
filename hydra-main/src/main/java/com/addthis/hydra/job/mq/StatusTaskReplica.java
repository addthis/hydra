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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = StatusTaskReplica.class)
public class StatusTaskReplica extends AbstractJobMessage {

    @JsonProperty private int version;
    @JsonProperty private long time;

    @JsonCreator
    private StatusTaskReplica() {
        super();
    }

    public StatusTaskReplica(String host, String job, int node, int version, long time) {
        super(host, job, node);
        this.version = version;
        this.time = time;
    }

    public int getVersion() {
        return version;
    }

    public long getUpdateTime() {
        return time;
    }
}

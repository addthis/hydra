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

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskDelete.class)
public class CommandTaskDelete extends AbstractJobMessage {

    @JsonProperty private int runCount;

    @JsonCreator
    private CommandTaskDelete() {
        super();
    }

    public CommandTaskDelete(String hostUuid, String job, @Nullable Integer node, int runCount) {
        super(hostUuid, job, node);
        this.runCount = runCount;
    }

    public int getRunCount() {
        return runCount;
    }

}

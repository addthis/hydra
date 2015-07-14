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

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskStop.class)
public class CommandTaskStop extends AbstractJobMessage {

    @JsonProperty private int runCount;
    @JsonProperty private boolean force;
    @JsonProperty private String choreWatcherKey = null;
    @JsonProperty private boolean onlyIfQueued = false;

    @JsonCreator
    private CommandTaskStop() {
        super();
    }

    public CommandTaskStop(String host, String job, Integer node, int runCount, boolean force, boolean onlyIfQueued) {
        super(host, job, node);
        this.runCount = runCount;
        this.force = force;
        this.onlyIfQueued = onlyIfQueued;
    }

    public int getRunCount() {
        return runCount;
    }

    public boolean force() {
        return force;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommandTaskStop that = (CommandTaskStop) o;

        if (force != that.force) return false;
        if (runCount != that.runCount) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = runCount;
        result = 31 * result + (force ? 1 : 0);
        return result;
    }

    public boolean getOnlyIfQueued() {
        return onlyIfQueued;
    }

    public String getChoreWatcherKey() {
        return choreWatcherKey;
    }
}

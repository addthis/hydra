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
package com.addthis.hydra.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The parameter here will be used when a job is created through the API
 * and does not have (key, value) pair for that parameter. The Spawn user
 * interface may provide its own default values for jobs created through
 * the UI so watch out for that. We are working on removing that behavior.
 */
public class JobDefaults {

    public final boolean ownerWritable;

    public final boolean groupWritable;

    public final boolean worldWritable;

    public final boolean ownerExecutable;

    public final boolean groupExecutable;

    public final boolean worldExecutable;

    public final int hourlyBackups;

    public final int dailyBackups;

    public final int weeklyBackups;

    public final int monthlyBackups;

    public final int replicas;

    @JsonCreator
    JobDefaults(@JsonProperty(value = "ownerWritable", required = true) boolean ownerWritable,
                @JsonProperty(value = "groupWritable", required = true) boolean groupWritable,
                @JsonProperty(value = "worldWritable", required = true) boolean worldWritable,
                @JsonProperty(value = "ownerExecutable", required = true) boolean ownerExecutable,
                @JsonProperty(value = "groupExecutable", required = true) boolean groupExecutable,
                @JsonProperty(value = "worldExecutable", required = true) boolean worldExecutable,
                @JsonProperty(value = "hourlyBackups", required = true) int hourlyBackups,
                @JsonProperty(value = "dailyBackups", required = true) int dailyBackups,
                @JsonProperty(value = "weeklyBackups", required = true) int weeklyBackups,
                @JsonProperty(value = "monthlyBackups", required = true) int monthlyBackups,
                @JsonProperty(value = "replicas", required = true) int replicas) {
        this.ownerWritable = ownerWritable;
        this.groupWritable = groupWritable;
        this.worldWritable = worldWritable;
        this.ownerExecutable = ownerExecutable;
        this.groupExecutable = groupExecutable;
        this.worldExecutable = worldExecutable;
        this.hourlyBackups = hourlyBackups;
        this.dailyBackups = dailyBackups;
        this.weeklyBackups = weeklyBackups;
        this.monthlyBackups = monthlyBackups;
        this.replicas = replicas;
    }


}

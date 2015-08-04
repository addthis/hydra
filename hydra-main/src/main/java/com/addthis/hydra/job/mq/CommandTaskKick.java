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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties({"killSignal", "retries"})
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, defaultImpl = CommandTaskKick.class)
public class CommandTaskKick implements JobMessage {

    @JsonProperty private String hostUuid;
    @JsonProperty private JobKey jobKey;
    @JsonProperty private String owner;
    @JsonProperty private String userGroup;
    @JsonProperty private int priority;
    @JsonProperty private int jobNodes;
    @JsonProperty private long runTime;
    @JsonProperty private Long submitTime;
    @JsonProperty private int runCount;
    @JsonProperty private int starts;
    @JsonProperty private String config;
    @JsonProperty private String command;
    @JsonProperty private int hourlyBackups;
    @JsonProperty private int dailyBackups;
    @JsonProperty private int weeklyBackups;
    @JsonProperty private int monthlyBackups;
    @JsonProperty private ReplicaTarget[] replicas;
    @JsonProperty private boolean autoRetry;

    @JsonCreator
    private CommandTaskKick() {}

    public CommandTaskKick(String host, JobKey jobKey, String owner, String userGroup, int priority, int jobNodes, long runTime,
                           int runCount, @Nullable String config, String command, int hourlyBackups,
                           int dailyBackups, int weeklyBackups, int monthlyBackups, @Nullable ReplicaTarget[] replicas,
                           boolean autoRetry, int starts) {
        this.hostUuid = host;
        this.jobKey = jobKey;
        this.owner = owner;
        this.userGroup = userGroup;
        this.priority = priority;
        this.jobNodes = jobNodes;
        this.runTime = runTime;
        this.submitTime = System.currentTimeMillis();
        this.runCount = runCount;
        this.starts = starts;
        this.config = config;
        this.command = command;
        this.hourlyBackups = hourlyBackups;
        this.dailyBackups = dailyBackups;
        this.weeklyBackups = weeklyBackups;
        this.monthlyBackups = monthlyBackups;
        this.replicas = replicas;
        this.autoRetry = autoRetry;
    }

    @Override @JsonIgnore
    public String getJobUuid() {
        return jobKey.getJobUuid();
    }

    @Override @JsonIgnore
    public Integer getNodeID() {
        return jobKey.getNodeNumber();
    }

    @Override public String getHostUuid() {
        return hostUuid;
    }

    @Override public JobKey getJobKey() {
        return jobKey;
    }

    public String getOwner() {
        return this.owner;
    }

    public String getUserGroup() {
        return this.userGroup;
    }
    public int getJobNodes() {
        return jobNodes;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setSubmitTime(long millis) {
        submitTime = millis;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public int getRunCount() {
        return runCount;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getCommand() {
        return command;
    }

    public int getPriority() {
        return priority;
    }

    public ReplicaTarget[] getReplicas() {
        return replicas;
    }

    public void setReplicas(ReplicaTarget[] replicas) {
        this.replicas = replicas;
    }

    public int getHourlyBackups() {
        return hourlyBackups;
    }

    public int getDailyBackups() {
        return dailyBackups;
    }

    public int getWeeklyBackups() {
        return weeklyBackups;
    }

    public int getMonthlyBackups() {
        return monthlyBackups;
    }

    public boolean getAutoRetry() {
        return autoRetry;
    }

    public void setAutoRetry(boolean autoRetry) {
        this.autoRetry = autoRetry;
    }

    public int getStarts() {
        return starts;
    }

    @Override
    public String toString() {
        return "[K|" + jobKey + "|" + jobNodes + "|" + priority + "]";
    }

    public String key() {
        return getJobKey().toString();
    }
}

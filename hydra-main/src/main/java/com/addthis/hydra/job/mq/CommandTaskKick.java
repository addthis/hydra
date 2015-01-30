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

import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"killSignal", "retries"})
public class CommandTaskKick implements JobMessage {

    private static final long serialVersionUID = -7588140676324569250L;

    @FieldConfig private String hostUuid;
    @FieldConfig private JobKey jobKey;
    @FieldConfig private int priority;
    @FieldConfig private int jobNodes;
    @FieldConfig private long runTime;
    @FieldConfig private Long submitTime;
    @FieldConfig private int runCount;
    @FieldConfig private int starts;
    @FieldConfig private String config;
    @FieldConfig private String command;
    @FieldConfig private int hourlyBackups;
    @FieldConfig private int dailyBackups;
    @FieldConfig private int weeklyBackups;
    @FieldConfig private int monthlyBackups;
    @FieldConfig private ReplicaTarget[] replicas;
    @FieldConfig private boolean autoRetry;

    @Override
    public String toString() {
        return "[K|" + jobKey + "|" + jobNodes + "|" + priority + "]";
    }

    public String key() {
        return getJobKey().toString();
    }

    public CommandTaskKick() {}

    public CommandTaskKick(String host, JobKey jobKey, int priority, int jobNodes, long runTime,
                           int runCount, String config, String command, int hourlyBackups,
                           int dailyBackups, int weeklyBackups, int monthlyBackups, ReplicaTarget[] replicas,
                           boolean autoRetry, int starts) {
        this.hostUuid = host;
        this.jobKey = jobKey;
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

    @Override @JsonIgnore
    public TYPE getMessageType() {
        return TYPE.CMD_TASK_KICK;
    }

    @Override public String getHostUuid() {
        return hostUuid;
    }

    @Override public JobKey getJobKey() {
        return jobKey;
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
}

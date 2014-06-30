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

import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;

public class CommandTaskKick implements JobMessage {

    private static final long serialVersionUID = -7588140676324569250L;

    @FieldConfig(codable = true)
    private String hostUuid;
    @FieldConfig(codable = true)
    private JobKey jobKey;
    @FieldConfig(codable = true)
    private int priority;
    @FieldConfig(codable = true)
    private int jobNodes;
    @FieldConfig(codable = true)
    private long runTime;
    @FieldConfig(codable = true)
    private Long submitTime;
    @FieldConfig(codable = true)
    private int runCount;
    @FieldConfig(codable = true)
    private String config;
    @FieldConfig(codable = true)
    private String command;
    @FieldConfig(codable = true)
    private String killSignal;
    @FieldConfig(codable = true)
    private int hourlyBackups;
    @FieldConfig(codable = true)
    private int dailyBackups;
    @FieldConfig(codable = true)
    private int weeklyBackups;
    @FieldConfig(codable = true)
    private int monthlyBackups;
    @FieldConfig(codable = true)
    private ReplicaTarget replicas[];
    @FieldConfig(codable = true)
    private int retries;

    @Override
    public String toString() {
        return "[K|" + jobKey + "|" + jobNodes + "|" + priority + "]";
    }

    public String key() {
        return getJobKey().toString();
    }

    public CommandTaskKick() {
    }

    public CommandTaskKick(String host, JobKey jobKey, int priority, int jobNodes, long runTime,
            int runCount, String config, String command, String killSignal, int hourlyBackups,
            int dailyBackups, int weeklyBackups, int monthlyBackups, ReplicaTarget replicas[]) {
        this.hostUuid = host;
        this.jobKey = jobKey;
        this.priority = priority;
        this.jobNodes = jobNodes;
        this.runTime = runTime;
        this.submitTime = System.currentTimeMillis();
        this.runCount = runCount;
        this.config = config;
        this.command = command;
        this.killSignal = killSignal;
        this.hourlyBackups = hourlyBackups;
        this.dailyBackups = dailyBackups;
        this.weeklyBackups = weeklyBackups;
        this.monthlyBackups = monthlyBackups;
        this.replicas = replicas;
    }

    @Override
    public String getJobUuid() {
        return jobKey.getJobUuid();
    }

    @Override
    public Integer getNodeID() {
        return jobKey.getNodeNumber();
    }

    @Override
    public TYPE getMessageType() {
        return TYPE.CMD_TASK_KICK;
    }

    public String getHostUuid() {
        return hostUuid;
    }

    public JobKey getJobKey() {
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

    public String getKillSignal() {
        return killSignal;
    }

    public void setKillSignal(String killSignal) {
        this.killSignal = killSignal;
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

    public int getBackups() {
        return hourlyBackups + dailyBackups + weeklyBackups;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
}

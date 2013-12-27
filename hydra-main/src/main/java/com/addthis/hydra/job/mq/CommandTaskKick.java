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

import com.addthis.codec.Codec;

public class CommandTaskKick implements JobMessage {

    private static final long serialVersionUID = -7588140676324569250L;

    @Codec.Set(codable = true)
    private String hostUuid;
    @Codec.Set(codable = true)
    private JobKey jobKey;
    @Codec.Set(codable = true)
    private int priority;
    @Codec.Set(codable = true)
    private int jobNodes;
    @Codec.Set(codable = true)
    private long runTime;
    @Codec.Set(codable = true)
    private Long submitTime;
    @Codec.Set(codable = true)
    private int runCount;
    @Codec.Set(codable = true)
    private String config;
    @Codec.Set(codable = true)
    private String command;
    @Codec.Set(codable = true)
    private String killSignal;
    @Codec.Set(codable = true)
    private HostCapacity capacity;
    @Codec.Set(codable = true)
    private int hourlyBackups;
    @Codec.Set(codable = true)
    private int dailyBackups;
    @Codec.Set(codable = true)
    private int weeklyBackups;
    @Codec.Set(codable = true)
    private int monthlyBackups;
    @Codec.Set(codable = true)
    private ReplicaTarget replicas[];
    @Codec.Set(codable = true)
    private boolean stomp;
    @Codec.Set(codable = true)
    private boolean force;
    @Codec.Set(codable = true)
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

    public CommandTaskKick(String host, JobKey jobKey, int priority, int jobNodes, HostCapacity capacity, long runTime,
            int runCount, String config, String command, String killSignal, int hourlyBackups,
            int dailyBackups, int weeklyBackups, int monthlyBackups, ReplicaTarget replicas[], boolean stomp, boolean force) {
        this.hostUuid = host;
        this.jobKey = jobKey;
        this.priority = priority;
        this.jobNodes = jobNodes;
        this.runTime = runTime;
        this.submitTime = System.currentTimeMillis();
        this.runCount = runCount;
        this.config = config;
        this.command = command;
        this.capacity = capacity;
        this.killSignal = killSignal;
        this.hourlyBackups = hourlyBackups;
        this.dailyBackups = dailyBackups;
        this.weeklyBackups = weeklyBackups;
        this.monthlyBackups = monthlyBackups;
        this.replicas = replicas;
        this.stomp = stomp;
        this.force = force;
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

    public HostCapacity getCapacity() {
        return capacity;
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

    public boolean getForce() {
        return force;
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

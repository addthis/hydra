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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.job.Minion;

import com.google.common.base.Objects;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties({"messageType", "totalLive"})
public class HostState implements HostMessage {

    private static final long serialVersionUID = 7252930788607795642L;

    @FieldConfig(codable = true)
    private String host;
    @FieldConfig(codable = true)
    private int port;
    @FieldConfig(codable = true)
    private String uuid;
    @FieldConfig(codable = true)
    private String user;
    @FieldConfig(codable = true)
    private String path;
    @FieldConfig(codable = true)
    private String group;
    // Host State is determined by others zk group membership, by
    // definition it can not persist.
    @FieldConfig(codable = true)
    private boolean up;
    @FieldConfig(codable = true)
    private long time;
    @FieldConfig(codable = true)
    private long uptime;
    @FieldConfig(codable = true)
    private int availableTaskSlots;
    @FieldConfig(codable = true)
    private int maxTaskSlots;
    @FieldConfig(codable = true)
    private JobKey running[];
    @FieldConfig(codable = true)
    private JobKey replicating[];
    @FieldConfig(codable = true)
    private JobKey backingup[];
    @FieldConfig(codable = true)
    private JobKey stopped[];
    @FieldConfig(codable = true)
    private JobKey replicas[];
    @FieldConfig(codable = true)
    private JobKey incompleteReplicas[];
    @FieldConfig(codable = true)
    private JobKey queued[];
    // Do not encode this derived, internal, non-typesafe field
    private HashMap<String, Integer> jobTaskCountMap;
    @FieldConfig(codable = true)
    private HostCapacity used;
    @FieldConfig(codable = true)
    private HostCapacity max;
    @FieldConfig(codable = true)
    private boolean dead;
    @FieldConfig(codable = true)
    private long lastUpdateTime;
    @FieldConfig(codable = true)
    private double histQueueSize;
    @FieldConfig(codable = true)
    private double histWaitTime;
    //TODO:  remove but need this in for now because de-serialization fails without it
    @FieldConfig(codable = true)
    private HashMap<String, Double> jobRuntimes = new HashMap<String, Double>();
    @FieldConfig(codable = true)
    private boolean readOnly;
    @FieldConfig(codable = true)
    private boolean diskReadOnly;
    @FieldConfig(codable = true)
    private boolean disabled;
    @FieldConfig(codable = true)
    private double meanActiveTasks;
    @FieldConfig(codable = true)
    private String minionTypes;

    public HostState() {
    }

    public HostState(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public TYPE getMessageType() {
        return TYPE.STATUS_HOST_INFO;
    }

    @Override
    public String getHostUuid() {
        return uuid;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public double getHistQueueSize() {
        return histQueueSize;
    }

    public void setHistQueueSize(double size) {
        this.histQueueSize = size;
    }

    public double getHistWaitTime() {
        return histWaitTime;
    }

    public void setHistWaitTime(double seconds) {
        this.histWaitTime = seconds;
    }

    public void setUpdated() {
        lastUpdateTime = System.currentTimeMillis();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @JsonProperty(value = "hostUuid")
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(boolean up) {
        this.up = up;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    public JobKey[] getRunning() {
        return running;
    }

    public void setRunning(JobKey[] running) {
        this.running = running;
    }

    // Needed for serialization!
    public JobKey[] getReplicating() {
        return replicating;
    }

    public void setReplicating(JobKey[] replicating) {
        this.replicating = replicating;
    }

    // Needed for serialization!
    public JobKey[] getBackingup() {
        return backingup;
    }

    public void setBackingUp(JobKey[] backingUp) {
        this.backingup = backingUp;
    }

    public JobKey[] getStopped() {
        return stopped;
    }

    public void setReplicas(JobKey[] replicas) {
        this.replicas = replicas;
    }

    public JobKey[] getReplicas() {
        return replicas;
    }

    public void setStopped(JobKey[] stopped) {
        this.stopped = stopped;
    }

    public JobKey[] getQueued() {
        return queued;
    }

    public void setQueued(JobKey[] queued) {
        this.queued = queued;
    }

    public HostCapacity getUsed() {
        return used;
    }

    public void setUsed(HostCapacity used) {
        this.used = used;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public HostCapacity getMax() {
        return max;
    }

    public void setMax(HostCapacity max) {
        this.max = max;
    }

    public void setDead(boolean dead) {
        this.dead = dead;
    }

    public boolean isDead() {
        return dead;
    }

    public boolean hasLive(JobKey jobKey) {
        if (stopped != null && Arrays.asList(stopped).contains(jobKey)) {
            return true;
        }
        if (queued != null && Arrays.asList(queued).contains(jobKey)) {
            return true;
        }
        if (running != null && Arrays.asList(running).contains(jobKey)) {
            return true;
        }
        if (replicating != null && Arrays.asList(replicating).contains(jobKey)) {
            return true;
        }
        if (backingup != null && Arrays.asList(backingup).contains(jobKey)) {
            return true;
        }
        return false;
    }

    public boolean hasIncompleteReplica(JobKey jobKey) {
        return (incompleteReplicas != null && Arrays.asList(incompleteReplicas).contains(jobKey));
    }

    public List<JobKey> allJobKeys() {
        List<JobKey> rv = new ArrayList<>();
        for (JobKey[] jobKeys : Arrays.asList(stopped, queued, running, replicating, backingup, replicas)) {
            if (jobKeys != null) {
                rv.addAll(Arrays.asList(jobKeys));
            }
        }
        return rv;
    }

    public Integer addJob(String jobId) {
        if (this.jobTaskCountMap == null) {
            jobTaskCountMap = new HashMap<String, Integer>();
        }
        int currentCount = 0;
        if (jobTaskCountMap.containsKey(jobId)) {
            currentCount = jobTaskCountMap.get(jobId);
        }
        return jobTaskCountMap.put(jobId, currentCount + 1);
    }

    public void generateJobTaskCountMap() {
        jobTaskCountMap = new HashMap<>();
        List<JobKey[]> activeJobsSources = Arrays.asList(stopped, queued, running, replicas);
        for (JobKey[] source : activeJobsSources) {
            if (source == null) {
                continue;
            }
            for (JobKey jobKey : source) {
                if (jobKey == null) {
                    continue;
                }
                Integer oldCount;
                int newCount = ((oldCount = jobTaskCountMap.get(jobKey.getJobUuid())) != null ? 1 + oldCount : 1);
                jobTaskCountMap.put(jobKey.getJobUuid(), newCount);
            }
        }
    }

    public Integer getTaskCount(String jobId) {
        if (jobTaskCountMap == null) {
            generateJobTaskCountMap();
        }
        return jobTaskCountMap != null && jobTaskCountMap.containsKey(jobId) ? jobTaskCountMap.get(jobId) : 0;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isDiskReadOnly() {
        return this.diskReadOnly;
    }

    public void setDiskReadOnly(boolean diskReadOnly) {
        this.diskReadOnly = diskReadOnly;
    }

    public int getAvailableTaskSlots() {
        return availableTaskSlots;
    }

    public void setAvailableTaskSlots(int availableTaskSlots) {
        this.availableTaskSlots = availableTaskSlots;
    }

    public boolean canMirrorTasks() {
        return (up && !dead && !diskReadOnly && !disabled);
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public double getMeanActiveTasks() {
        return meanActiveTasks;
    }

    public void setMeanActiveTasks(double meanActiveTasks) {
        this.meanActiveTasks = meanActiveTasks;
    }

    public String getMinionTypes() {
        return minionTypes;
    }

    public void setMinionTypes(String minionTypes) {
        this.minionTypes = minionTypes;
    }

    public boolean hasType(String type) {
        if (minionTypes == null) {
            minionTypes = Minion.getDefaultMinionType();
        }
        if (minionTypes.contains(",")) {
            return Arrays.asList(minionTypes.split(",")).contains(type);
        }
        return type.equals(minionTypes);
    }

    public JobKey[] getIncompleteReplicas() {
        return incompleteReplicas;
    }

    public void setIncompleteReplicas(JobKey[] incompleteReplicas) {
        this.incompleteReplicas = incompleteReplicas;
    }

    public int getMaxTaskSlots() {
        return maxTaskSlots;
    }

    public void setMaxTaskSlots(int maxTaskSlots) {
        this.maxTaskSlots = maxTaskSlots;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", getMessageType())
                .add("uuid", getHostUuid())
                .add("last-update-time", getLastUpdateTime())
                .add("host", getHost())
                .add("port", getPort())
                .add("group", getGroup())
                .add("time", getTime())
                .add("uptime", getUptime())
                        // You probably only want to print these out when testing locally
                        // .add("running", getRunning())
                        // .add("stopped", getStopped())
                        // .add("replicas", getReplicas())
                        // .add("queued",getQueued())
                .add("used", getUsed())
                .add("user", getUser())
                .add("path", getPath())
                .add("max", getMax())
                .add("up", isUp())
                .add("dead", isDead())
                .add("readOnly", isReadOnly())
                .add("diskReadOnly", isDiskReadOnly())
                .toString();
    }

    public int countTotalLive() {
        int total = 0;
        for (JobKey[] keys : Arrays.asList(stopped, running, replicating, backingup, queued)) {
            if (keys != null) {
                total += keys.length;
            }
        }
        return total;
    }
}

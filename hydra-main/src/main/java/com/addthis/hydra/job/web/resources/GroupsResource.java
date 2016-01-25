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
package com.addthis.hydra.job.web.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.SpawnServiceConfiguration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@javax.ws.rs.Path("/group")
public class GroupsResource {

    private static final Logger log = LoggerFactory.getLogger(GroupsResource.class);

    private static final String DEFAULT_GROUP = "UNKNOWN";

    private static final String LOG_FILENAME = "disk-usage.txt";

    private static final ScheduledExecutorService EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("spawn-group-resource")
                            .build());

    private final Spawn spawn;

    private final Path logDirectory;

    private volatile double adjustedRatio;

    private volatile ImmutableMap<String, ImmutableList<MinimalJob>> diskUsage;

    private volatile ImmutableMap<String, Double> diskSummary;

    public GroupsResource(Spawn spawn, SpawnServiceConfiguration configuration) {
        this.spawn = spawn;
        this.diskUsage = ImmutableMap.of();
        this.diskSummary = ImmutableMap.of();
        this.logDirectory = (configuration.groupLogDir != null) ? Paths.get(configuration.groupLogDir) : null;
        EXECUTOR.scheduleWithFixedDelay(this::updateDiskQuotas,
                                        configuration.groupUpdateInterval,
                                        configuration.groupUpdateInterval,
                                        TimeUnit.SECONDS);
        EXECUTOR.scheduleWithFixedDelay(this::logDiskQuotas,
                                        configuration.groupLogInterval,
                                        configuration.groupLogInterval,
                                        TimeUnit.SECONDS);
    }

    @SuppressWarnings("unused")
    private static class MinimalJob {
        final String id;
        final String creator;
        final String owner;
        final String description;
        final long createTimeMillis;
        final String createTime;
        final long rawBytes;
        long adjustedBytes;
        double percentileBytes;
        final String link;

        MinimalJob(Spawn spawn, Job job, long rawBytes) {
            this.id = job.getId();
            this.creator = job.getCreator();
            this.owner = job.getOwner();
            this.description = job.getDescription();
            this.createTimeMillis = job.getCreateTime();
            Instant instant = Instant.ofEpochMilli(createTimeMillis);
            ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
            this.createTime = dateTime.format(DateTimeFormatter.RFC_1123_DATE_TIME);
            this.rawBytes = rawBytes;
            this.link = "http://" + spawn.getSystemManager().getSpawnHost() +
                        "/spawn2/index.html#jobs/" + id + "/conf";
        }

        public String getId() {
            return id;
        }

        public String getCreator() {
            return creator;
        }

        public String getOwner() {
            return owner;
        }

        public String getDescription() {
            return description;
        }

        public String getCreateTime() {
            return createTime;
        }

        public long getRawBytes() {
            return rawBytes;
        }

        public long getAdjustedBytes() {
            return adjustedBytes;
        }

        public double getPercentileBytes() {
            return percentileBytes;
        }

        public String getLink() {
            return link;
        }

        void adjustBytes(double adjustedRatio, long diskCapacity) {
            adjustedBytes = (long) (rawBytes * adjustedRatio);
            percentileBytes = adjustedBytes / ((double) diskCapacity);
        }
    }

    private static final Comparator<MinimalJob> DISK_USAGE_COMPARATOR =
            (j1, j2) -> Long.compare(j2.rawBytes, j1.rawBytes);

    private static final Comparator<MinimalJob> CREATE_TIME_COMPARATOR =
            (j1, j2) -> Long.compare(j2.createTimeMillis, j1.createTimeMillis);

    private void logDiskQuotas() {
        try {
            if (logDirectory == null) {
                return;
            }
            Files.createDirectories(logDirectory);
            Path logfile = logDirectory.resolve(LOG_FILENAME);
            long timestamp = System.currentTimeMillis();
            ImmutableMap<String, Double> summary = this.diskSummary;
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(logfile,
                                                                              StandardOpenOption.CREATE,
                                                                              StandardOpenOption.WRITE,
                                                                              StandardOpenOption.APPEND))) {
                for (Map.Entry<String, Double> entry : summary.entrySet()) {
                    writer.printf("%d\t%s\t%.3f%n", timestamp, entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception ex) {
            log.warn("Error logging group resource: ", ex);
        }
    }

    private void updateDiskQuotas() {
        try {
            long diskUsed = 0;
            long diskCapacity = 0;
            for (HostState host : spawn.hostManager.getLiveHosts(null)) {
                diskUsed += host.getUsed().getDisk();
                diskCapacity += host.getMax().getDisk();
            }
            Map<String, List<MinimalJob>> quotas = new HashMap<>();
            long totalBytes = 0;
            spawn.acquireJobLock();
            try {
                Iterator<Job> jobIterator = spawn.getSpawnState().jobsIterator();
                while (jobIterator.hasNext()) {
                    Job job = jobIterator.next();
                    String group = job.getGroup();
                    if (Strings.isNullOrEmpty(group)) {
                        group = DEFAULT_GROUP;
                    }
                    List<MinimalJob> groupJobs = quotas.computeIfAbsent(group, (k) -> new ArrayList<>());
                    long bytes = 0;
                    for (JobTask jobTask : job.getCopyOfTasks()) {
                        bytes += jobTask.getByteCount();
                    }
                    totalBytes += bytes;
                    MinimalJob minimalJob = new MinimalJob(spawn, job, bytes);
                    groupJobs.add(minimalJob);
                }
            } finally {
                spawn.releaseJobLock();
            }
            adjustedRatio = diskUsed / ((double) totalBytes);
            for (Collection<MinimalJob> jobs : quotas.values()) {
                for (MinimalJob job : jobs) {
                    job.adjustBytes(adjustedRatio, diskCapacity);
                }
            }
            ImmutableMap.Builder<String, ImmutableList<MinimalJob>> diskUsageBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, Double> diskSummaryBuilder = ImmutableMap.builder();
            for (Map.Entry<String, List<MinimalJob>> entry : quotas.entrySet()) {
                diskUsageBuilder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
                long groupBytes = 0;
                for (MinimalJob job : entry.getValue()) {
                    groupBytes += job.adjustedBytes;
                }
                diskSummaryBuilder.put(entry.getKey(), groupBytes / ((double) diskCapacity));
            }
            diskUsage = diskUsageBuilder.build();
            diskSummary = diskSummaryBuilder.build();
        } catch (Exception ex) {
            log.warn("Error updating group resource: ", ex);
        }
    }

    @GET
    @javax.ws.rs.Path("/disk/ratio")
    @Produces(MediaType.APPLICATION_JSON)
    public double getAdjustedRatio() {
        return adjustedRatio;
    }

    private Map<String, List<MinimalJob>> generateUsageOutput(Comparator<MinimalJob> comparator,
                                                              String group,
                                                              int limit) {
        ImmutableMap<String, ImmutableList<MinimalJob>> data = diskUsage;
        Map<String, List<MinimalJob>> result = new HashMap<>();
        if (group != null) {
            List<MinimalJob> list = data.get(group);
            if (list != null) {
                Collections.sort(list, comparator);
                if (limit > 0) {
                    list = list.subList(0, limit);
                }
                result.put(group, list);
            }
        } else {
            for (Map.Entry<String, ImmutableList<MinimalJob>> entry : data.entrySet()) {
                List<MinimalJob> list = new ArrayList<>(entry.getValue());
                Collections.sort(list, comparator);
                if (limit > 0) {
                    list = list.subList(0, limit);
                }
                result.put(entry.getKey(), list);
            }
        }
        return result;
    }

    @GET
    @javax.ws.rs.Path("/disk/usage/size")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, List<MinimalJob>> getDiskUsageBySize(@QueryParam("group") String group,
                                                            @QueryParam("limit") int limit) {
        return generateUsageOutput(DISK_USAGE_COMPARATOR, group, limit);
    }

    @GET
    @javax.ws.rs.Path("/disk/usage/date")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, List<MinimalJob>> getDiskUsageByDate(@QueryParam("group") String group,
                                                            @QueryParam("limit") int limit) {
        return generateUsageOutput(CREATE_TIME_COMPARATOR, group, limit);
    }

    @GET
    @javax.ws.rs.Path("/disk/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public ImmutableMap<String, Double> getDiskSummary() {
        return diskSummary;
    }

}

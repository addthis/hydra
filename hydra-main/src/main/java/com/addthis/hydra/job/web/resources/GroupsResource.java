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
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

@Path("/group")
public class GroupsResource {

    private static final Logger log = LoggerFactory.getLogger(GroupsResource.class);

    private static final ScheduledExecutorService EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("spawn-group-resource")
                            .build());

    private static final String DEFAULT_GROUP = "UNKNOWN";

    private final Spawn spawn;

    private volatile double adjustedRatio;

    private volatile ImmutableMap<String, ImmutableList<MinimalJob>> diskUsage;

    private volatile ImmutableMap<String, Double> diskSummary;

    public GroupsResource(Spawn spawn, SpawnServiceConfiguration configuration) {
        this.spawn = spawn;
        this.diskUsage = ImmutableMap.of();
        this.diskSummary = ImmutableMap.of();
        EXECUTOR.scheduleWithFixedDelay(this::updateDiskQuotas,
                                        configuration.groupResourceUpdateInterval,
                                        configuration.groupResourceUpdateInterval,
                                        TimeUnit.SECONDS);
    }

    private static class MinimalJob {
        final String id;
        final String description;
        final long rawBytes;
        long adjustedBytes;

        MinimalJob(String id, String description, long rawTotal) {
            this.id = id;
            this.description = description;
            this.rawBytes = rawTotal;
        }

        public String getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }

        public long getRawBytes() {
            return rawBytes;
        }

        public long getAdjustedBytes() {
            return adjustedBytes;
        }
    }

    private static final Comparator<MinimalJob> DISK_USAGE_COMPARATOR =
            (j1, j2) -> Long.compare(j2.rawBytes, j1.rawBytes);

    private void updateDiskQuotas() {
        try {
            long diskUsed = 0;
            long diskCapacity = 0;
            for (HostState host : spawn.hostManager.getLiveHosts(null)) {
                diskUsed += host.getUsed().getDisk();
                diskCapacity += host.getMax().getDisk();
            }
            Map<String, SortedSet<MinimalJob>> quotas = new HashMap<>();
            long totalBytes = 0;
            spawn.acquireJobLock();
            try {
                Iterator<Job> jobIterator = spawn.getSpawnState().jobsIterator();
                while (jobIterator.hasNext()) {
                    Job job = jobIterator.next();
                    String id = job.getId();
                    String description = job.getDescription();
                    String group = job.getGroup();
                    if (Strings.isNullOrEmpty(group)) {
                        group = DEFAULT_GROUP;
                    }
                    SortedSet<MinimalJob> groupJobs = quotas.computeIfAbsent(group, (k) ->
                            new TreeSet<>(DISK_USAGE_COMPARATOR));
                    long bytes = 0;
                    for (JobTask jobTask : job.getCopyOfTasks()) {
                        bytes += jobTask.getByteCount();
                    }
                    totalBytes += bytes;
                    MinimalJob minimalJob = new MinimalJob(id, description, bytes);
                    groupJobs.add(minimalJob);
                }
            } finally {
                spawn.releaseJobLock();
            }
            adjustedRatio = diskUsed / ((double) totalBytes);
            for (Collection<MinimalJob> jobs : quotas.values()) {
                for (MinimalJob job : jobs) {
                    job.adjustedBytes = (long) (job.rawBytes * adjustedRatio);
                }
            }
            ImmutableMap.Builder<String, ImmutableList<MinimalJob>> diskUsageBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, Double> diskSummaryBuilder = ImmutableMap.builder();
            for (Map.Entry<String, SortedSet<MinimalJob>> entry : quotas.entrySet()) {
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
    @Path("/disk/ratio")
    @Produces(MediaType.APPLICATION_JSON)
    public double getAdjustedRatio() {
        return adjustedRatio;
    }

    @GET
    @Path("/disk/usage")
    @Produces(MediaType.APPLICATION_JSON)
    public ImmutableMap<String, ImmutableList<MinimalJob>> getDiskUsage() {
        return diskUsage;
    }

    @GET
    @Path("/disk/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public ImmutableMap<String, Double> getDiskSummary() {
        return diskSummary;
    }

}

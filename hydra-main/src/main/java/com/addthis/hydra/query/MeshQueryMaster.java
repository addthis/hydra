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
package com.addthis.hydra.query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.addthis.basis.LessStreams;
import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.query.aggregate.BalancedAllocator;
import com.addthis.hydra.query.aggregate.DefaultTaskAllocators;
import com.addthis.hydra.query.aggregate.MeshSourceAggregator;
import com.addthis.hydra.query.aggregate.QueryTaskSource;
import com.addthis.hydra.query.aggregate.QueryTaskSourceOption;
import com.addthis.hydra.query.loadbalance.WorkerData;
import com.addthis.hydra.query.loadbalance.WorkerTracker;
import com.addthis.hydra.query.spawndatastore.SpawnDataStoreHandler;
import com.addthis.hydra.query.tracker.QueryTracker;
import com.addthis.hydra.query.tracker.TrackerHandler;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

@ChannelHandler.Sharable
public class MeshQueryMaster extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MeshQueryMaster.class);

    private static final String  tempDir         = Parameter.value("query.tmpdir", "query.tmpdir");
    private static final int     meshPort        = Parameter.intValue("qmaster.mesh.port", 5100);
    private static final String  meshRoot        = Parameter.value("qmaster.mesh.root", "/home/hydra");
    private static final String  meshPeers       = Parameter.value("qmaster.mesh.peers", "localhost");
    private static final int     meshPeerPort    = Parameter.intValue("qmaster.mesh.peer.port", 5101);
    private static final boolean enableZooKeeper = Parameter.boolValue("qmaster.enableZooKeeper", true);

    private static final QueryTaskSource EMPTY_TASK_SOURCE = new QueryTaskSource(new QueryTaskSourceOption[0]);

    /**
     * used for tracking metrics and other interesting things about queries that we have run.
     * Provides insight into currently running queries and gives ability to cancel a query before it completes.
     */
    private final QueryTracker tracker;

    /** Primary Mesh server */
    private final MeshyServer meshy;

    /** Abstracts away spawndatastore-reliant functions */
    private final SpawnDataStoreHandler spawnDataStoreHandler;

    /** Mesh FileRef Cache -- backed by a loading cache */
    private final MeshFileRefCache cachey;

    private final WorkerTracker worky;
    private final DefaultTaskAllocators allocators;

    public MeshQueryMaster(QueryTracker tracker) throws Exception {
        this.tracker = tracker;

        meshy = new MeshyServer(meshPort, new File(meshRoot));
        cachey = new MeshFileRefCache(meshy);
        worky = new WorkerTracker();
        allocators = new DefaultTaskAllocators(new BalancedAllocator(worky));
        connectToMeshPeers();

        try {
            // Delete the tmp directory (disk sort directory)
            File tempDirFile = new File(tempDir).getCanonicalFile();
            LessFiles.deleteDir(tempDirFile);
            LessFiles.initDirectory(tempDirFile);
        } catch (Exception e) {
            log.warn("Error while cleaning / locating the temp directory (for disk sorts).", e);
        }

        if (enableZooKeeper) {
            spawnDataStoreHandler = new SpawnDataStoreHandler();
        } else {
            spawnDataStoreHandler = null;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    public SpawnDataStoreHandler getSpawnDataStoreHandler() {
        return spawnDataStoreHandler;
    }

    public DefaultTaskAllocators allocators() {
        return allocators;
    }

    public WorkerTracker worky() {
        return worky;
    }

    public QueryTracker getQueryTracker() {
        return tracker;
    }

    protected void shutdown() {
        try {
            if (spawnDataStoreHandler != null) {
                spawnDataStoreHandler.close();
            }
            meshy.close();
            if (tracker != null) {
                tracker.close();
            }
        } catch (Exception e) {
            log.error("arbitrary exception during mqmaster shutdown", e);
        }
    }

    private void connectToMeshPeers() {
        if (meshPeers != null) {
            String[] peers = meshPeers.split(",");
            for (String peer : peers) {
                meshy.connectPeer(new InetSocketAddress(peer, meshPeerPort));
            }
        }
    }

    public void handleError(Query query) {
        String job = query.getJob();
        if (job != null) {
            cachey.invalidate(job);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Query) {
            writeQuery(ctx, (Query) msg, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    private static String getJobSubdirectory(String combinedJob) {
        int dirIndex = combinedJob.indexOf('/');
        if (dirIndex > -1) {
            return combinedJob.substring(dirIndex + 1);
        } else {
            return "";
        }
    }

    private static String getJobWithoutSubdirectory(String combinedJob) {
        int dirIndex = combinedJob.indexOf('/');
        if (dirIndex > -1) {
            return combinedJob.substring(0, dirIndex);
        } else {
            return combinedJob;
        }
    }

    private static final Splitter JOB_SPLITTER = Splitter.on(',');

    protected void writeQuery(ChannelHandlerContext ctx, Query query, ChannelPromise promise) throws Exception {
        // log rops prior to mutating query
        String[] opsLog = query.getOps();
        // creates query for worker and updates local query ops (!mutates query!)
        Query remoteQuery = query.createPipelinedQuery();


        boolean allowPartial = Boolean.valueOf(query.getParameter("allowPartial"));
        Set<Integer> tasks = parseTasks(query.getParameter("tasks"));
        List<QueryTaskSource[]> sourcesPerDir = new ArrayList<>(2);
        for (String combinedUnresolved : JOB_SPLITTER.split(query.getJob())) {
            String jobIdOrAlias = getJobWithoutSubdirectory(combinedUnresolved);
            String subdirectory = getJobSubdirectory(combinedUnresolved);
            for (String resolved : expandAlias(jobIdOrAlias)) {
                String resolvedJobId = getJobWithoutSubdirectory(resolved);
                String resolvedSubdirectory;
                if (!subdirectory.isEmpty()) {
                    resolvedSubdirectory = subdirectory;
                } else {
                    resolvedSubdirectory = getJobSubdirectory(resolved);
                }

                sourcesPerDir.add(getSourcesById(resolvedJobId, resolvedSubdirectory, allowPartial, tasks));
            }
        }
        QueryTaskSource[] sourcesByTaskID;
        if (sourcesPerDir.size() > 1) {
            sourcesByTaskID = sourcesPerDir.stream().flatMap(Arrays::stream).toArray(QueryTaskSource[]::new);
        } else {
            sourcesByTaskID = sourcesPerDir.get(0);
        }

        MeshSourceAggregator aggregator = new MeshSourceAggregator(sourcesByTaskID, meshy, this, remoteQuery);
        ctx.pipeline().addLast(ctx.executor(), "query aggregator", aggregator);
        TrackerHandler trackerHandler = new TrackerHandler(tracker, opsLog);
        ctx.pipeline().addLast(ctx.executor(), "query tracker", trackerHandler);
        ctx.pipeline().remove(this);
        ctx.pipeline().write(query, promise);
    }

    @Nonnull private static Set<Integer> parseTasks(@Nullable String tasks) {
        if (Strings.isNullOrEmpty(tasks)) {
            return Collections.emptySet();
        } else {
            return LessStreams.stream(Splitter.on(',').trimResults().split(tasks))
                              .map(Ints::tryParse)
                              .filter(i -> i != null)
                              .collect(Collectors.toSet());
        }
    }

    private List<String> expandAlias(String jobId) {
        if (spawnDataStoreHandler != null) {
            return spawnDataStoreHandler.expandAlias(jobId);
        } else {
            return Collections.singletonList(jobId);
        }
    }

    /**
     * @param requestedTasks    only query these task ids. empty means query all tasks.
     */
    private QueryTaskSource[] getSourcesById(String jobId,
                                             String subdirectory,
                                             boolean allowPartial,
                                             Set<Integer> requestedTasks) {
        if (spawnDataStoreHandler != null) {
            spawnDataStoreHandler.validateJobForQuery(jobId);
        }

        String combinedJob = subdirectory.isEmpty() ? jobId : jobId + '/' + subdirectory;

        Multimap<Integer, FileReference> fileReferenceMap;
        try {
            fileReferenceMap = cachey.get(combinedJob);
        } catch (ExecutionException e) {
            log.warn("", e);
            throw new QueryException("Exception getting file references: " + e.getMessage());
        }
        if ((fileReferenceMap == null) || fileReferenceMap.isEmpty()) {
            cachey.invalidate(combinedJob);
            throw new QueryException("[MeshQueryMaster] No file references found for job: " + combinedJob);
        }

        int canonicalTaskCount;
        try {
            canonicalTaskCount = validateRequestedTasks(jobId, fileReferenceMap.keySet(), requestedTasks, allowPartial);
        } catch (Exception ex) {
            cachey.invalidate(combinedJob);
            throw ex;
        }

        QueryTaskSource[] sourcesByTaskID = new QueryTaskSource[canonicalTaskCount];
        for (int taskId = 0; taskId < canonicalTaskCount; taskId++) {
            Collection<FileReference> sourceOptions = fileReferenceMap.get(taskId);
            if (!sourceOptions.isEmpty() && (requestedTasks.isEmpty() || requestedTasks.contains(taskId))) {
                QueryTaskSourceOption[] taskSourceOptions = new QueryTaskSourceOption[sourceOptions.size()];
                int taskSourceOptionsIndex = 0;
                for (FileReference queryReference : sourceOptions) {
                    WorkerData workerData = worky.get(queryReference.getHostUUID());
                    taskSourceOptions[taskSourceOptionsIndex] =
                            new QueryTaskSourceOption(queryReference, workerData.queryLeases);
                    taskSourceOptionsIndex += 1;
                }
                sourcesByTaskID[taskId] = new QueryTaskSource(taskSourceOptions);
            } else {
                sourcesByTaskID[taskId] = EMPTY_TASK_SOURCE;
            }
        }

        return sourcesByTaskID;
    }

    @VisibleForTesting
    protected int validateRequestedTasks(String jobId,
                                       Set<Integer> availableTasks,
                                       Set<Integer> requestedTasks,
                                       boolean allowPartial) {
        int canonicalTasks;
        if (spawnDataStoreHandler != null) {
            canonicalTasks = spawnDataStoreHandler.getCononicalTaskCount(jobId);
        } else {
            // the best guess is that there are at least max_available_task_id + 1 tasks
            canonicalTasks = Collections.max(availableTasks) + 1;
        }
        validateRequestedTasks(canonicalTasks, availableTasks, requestedTasks, allowPartial);
        return canonicalTasks;
    }

    /**
     * Validates if all requested tasks are available.
     *
     * @param canonicalTaskCount    total number of tasks.
     * @param availableTasks        available task ids.
     * @param tasks                 requested tasks ids. If empty, all tasks are requested, i.e. 0 to
     *                              {@code canonicalTaskCount-1}
     */
    private void validateRequestedTasks(int canonicalTaskCount,
                                        Set<Integer> availableTasks,
                                        Set<Integer> tasks,
                                        boolean allowPartial) {
        if (availableTasks.size() != canonicalTaskCount) {
            Set<Integer> requestedTasks = expandRequestedTasks(tasks, canonicalTaskCount);
            Set<Integer> missingTasks = new TreeSet<>(Sets.difference(requestedTasks, availableTasks));
            if (!allowPartial && !missingTasks.isEmpty()) {
                // if allowPartial = false, fail if any requested task is unavailable
                throw new QueryException("Did not find data for all " + requestedTasks.size() +
                                         " requested tasks (and allowPartial is off): " + availableTasks.size() +
                                         " available out of " + canonicalTaskCount + " total. Missing the following " +
                                         missingTasks.size() + " tasks: " + missingTasks);
            } else if (allowPartial && requestedTasks.size() == missingTasks.size()) {
                // if allowPartial = true, fail only if all requested tasks are unavailable
                throw new QueryException("Did not find data for any of the " + requestedTasks.size() +
                                         " requested tasks (and allowPartial is on): " + availableTasks.size() +
                                         " available out of " + canonicalTaskCount + " total. Missing the following " +
                                         missingTasks.size() + " tasks: " + missingTasks);
            }
        }
    }

    /** Returns the specified requested tasks as is if not empty, or expands to all known tasks if it is empty. */
    private Set<Integer> expandRequestedTasks(Set<Integer> tasks, int canonicalTaskCount) {
        if (tasks.isEmpty()) {
            return ContiguousSet.create(Range.closedOpen(0, canonicalTaskCount), DiscreteDomain.integers());
        } else {
            return tasks;
        }
    }

    /**
     * Called after MeshSourceAggregator detects that one of the FileReferences in the cache is invalid/out of date.
     * Look for an alternate FileReferenceWrapper in the cache. If none exists, fetch a replacement via a mesh lookup.
     *
     * @param failedReference The FileReference that threw the exception
     * @return A replacement FileReference, which is also placed into the cache if it was newly generated
     * @throws IOException If there is a problem fetching a replacement FileReference
     */
    public QueryTaskSourceOption getReplacementQueryTaskOption(FileReference failedReference)
            throws IOException, ExecutionException, InterruptedException {
        List<String> pathTokens = tokenizePath(failedReference.name);
        String job = getJobFromPath(pathTokens);
        int task = getTaskFromPath(pathTokens);

        Set<FileReference> oldReferences = cachey.getTaskReferencesIfPresent(job, task);
        Set<FileReference> newReferences = new HashSet<>(oldReferences);
        newReferences.remove(failedReference);
        if (newReferences.isEmpty()) {
            // there was no replacement fileReference in the cache, so we need to fetch a new one
            FileReference freshFileReference = cachey.getFileReferenceForSingleTask(job, task);
            newReferences.add(freshFileReference);
        }
        cachey.updateFileReferenceForTask(job, task, newReferences);
        FileReference cachedReplacement = newReferences.iterator().next();
        WorkerData workerData = worky.get(cachedReplacement.getHostUUID());
        return new QueryTaskSourceOption(cachedReplacement, workerData.queryLeases);
    }

    // omit empty strings so that we don't have to worry about random "//" instead of "/" or leading "/"s
    private static final Splitter FILEREF_PATH_SPLITTER = Splitter.on('/').omitEmptyStrings().limit(5);

    @VisibleForTesting static List<String> tokenizePath(String path) {
       return FILEREF_PATH_SPLITTER.splitToList(path);
    }

    @VisibleForTesting static String getJobFromPath(List<String> pathTokens) {
        String jobId = pathTokens.get(1);
        String jobDirWithSuffix = pathTokens.get(4);
        String jobDir = jobDirWithSuffix.substring(0, jobDirWithSuffix.length() - 6);
        return jobId + '/' + jobDir;
    }

    @VisibleForTesting static int getTaskFromPath(List<String> pathTokens) {
        return Integer.parseInt(pathTokens.get(2));
    }
}

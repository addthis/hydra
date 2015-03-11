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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
import com.google.common.collect.Multimap;
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
        for (String combinedJobOrAlias : JOB_SPLITTER.split(query.getJob())) {
            String jobIdOrAlias = getJobWithoutSubdirectory(combinedJobOrAlias);
            String combinedSubdirectory = getJobSubdirectory(combinedJobOrAlias);
            for (String combinedJob : expandAlias(jobIdOrAlias)) {
                String jobId = getJobWithoutSubdirectory(combinedJob);
                String jobSubdirectory;
                if (!combinedSubdirectory.isEmpty()) {
                    jobSubdirectory = combinedSubdirectory;
                } else {
                    jobSubdirectory = getJobSubdirectory(combinedJob);
                }

                sourcesPerDir.add(getSourcesById(jobId, jobSubdirectory, allowPartial, tasks));
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

    private static Set<Integer> parseTasks(String tasks) {
        if (Strings.isNullOrEmpty(tasks)) {
            return Collections.emptySet();
        } else {
            return Sets.newHashSet(Splitter.on(',').trimResults().split(tasks)).stream()
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
     * @param tasks only query these task ids. empty means query all tasks.
     */
    private QueryTaskSource[] getSourcesById(String jobId,
                                             String subdirectory,
                                             boolean allowPartial,
                                             Set<Integer> tasks) {
        if (spawnDataStoreHandler != null) {
            spawnDataStoreHandler.validateJobForQuery(jobId);
        }

        String combinedJob;
        if (!subdirectory.isEmpty()) {
            combinedJob = jobId + '/' + subdirectory;
        } else {
            combinedJob = jobId;
        }

        Multimap<Integer, FileReference> fileReferenceMap;
        try {
            fileReferenceMap = cachey.get(combinedJob);
            if ((fileReferenceMap == null) || fileReferenceMap.isEmpty()) {
                cachey.invalidate(combinedJob);
                throw new QueryException("[MeshQueryMaster] No file references found for job: " + combinedJob);
            }
        } catch (ExecutionException e) {
            log.warn("", e);
            throw new QueryException("Exception getting file references: " + e.getMessage());
        }

        int canonicalTasks;
        if (!allowPartial && (spawnDataStoreHandler != null)) {
            try {
                canonicalTasks = spawnDataStoreHandler.validateTaskCount(jobId, fileReferenceMap, tasks);
            } catch (Exception ex) {
                cachey.invalidate(combinedJob);
                throw ex;
            }
        } else {
            // task-ids are zero indexed, so add one
            canonicalTasks = fileReferenceMap.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
        }

        QueryTaskSource[] sourcesByTaskID = new QueryTaskSource[canonicalTasks];
        for (int i = 0; i < canonicalTasks; i++) {
            Collection<FileReference> sourceOptions = fileReferenceMap.get(i);
            if (sourceOptions != null && (tasks.isEmpty() || tasks.contains(i))) {
                QueryTaskSourceOption[] taskSourceOptions = new QueryTaskSourceOption[sourceOptions.size()];
                int taskSourceOptionsIndex = 0;
                for (FileReference queryReference : sourceOptions) {
                    WorkerData workerData = worky.get(queryReference.getHostUUID());
                    taskSourceOptions[taskSourceOptionsIndex] =
                            new QueryTaskSourceOption(queryReference, workerData.queryLeases);
                    taskSourceOptionsIndex += 1;
                }
                sourcesByTaskID[i] = new QueryTaskSource(taskSourceOptions);
            } else {
                sourcesByTaskID[i] = EMPTY_TASK_SOURCE;
            }
        }

        return sourcesByTaskID;
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

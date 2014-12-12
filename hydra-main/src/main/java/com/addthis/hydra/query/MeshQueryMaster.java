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

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.discovery.MesosServiceDiscoveryUtility;
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
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
public class MeshQueryMaster extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MeshQueryMaster.class);

    private static final String  tempDir           = Parameter.value("query.tmpdir", "query.tmpdir");
    private static final int     meshPort          = Parameter.intValue("qmaster.mesh.port", 5100);
    private static final String  meshRoot          = Parameter.value("qmaster.mesh.root", "/home/hydra");
    private static final String  meshPeers         = Parameter.value("qmaster.mesh.peers", "localhost");
    private static final int     meshPeerPort      = Parameter.intValue("qmaster.mesh.peer.port", 5101);
    private static final int     meshPeerInterval  = Parameter.intValue("qmaster.mesh.peer.interval", 60);
    private static final boolean enableZooKeeper   = Parameter.boolValue("qmaster.enableZooKeeper", true);
    private static final boolean useMesos          = Parameter.boolValue("qmaster.useMesos", false);
    private static final String  mqWorkerAppName   = Parameter.value("qmaster.mqWorkerAppName", "hydraworker");
    private static final int     mqWorkerPortIndex = Parameter.intValue("qmaster.mwWorkerAppPortIndex", 1);

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

        /*
         * thread pool for peering
         */
        ScheduledExecutorService queryMeshPeerMaintainer = MoreExecutors
                .getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                        new ThreadFactoryBuilder().setNameFormat("queryMeshPeerMaintainer=%d").build()));
        queryMeshPeerMaintainer.scheduleAtFixedRate(this::connectToMeshPeers, 0, meshPeerInterval, TimeUnit.SECONDS);
        try {
            // Delete the tmp directory (disk sort directory)
            File tempDirFile = new File(tempDir).getCanonicalFile();
            Files.deleteDir(tempDirFile);
            Files.initDirectory(tempDirFile);
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
        if (useMesos) {
            Optional<Map<String, Integer>> peerMap = MesosServiceDiscoveryUtility.getTaskHosts(mqWorkerAppName, mqWorkerPortIndex);
            if (peerMap.isPresent()) {
                Map<String, Integer> m = peerMap.get();
                for (Map.Entry<String, Integer> entry : m.entrySet()) {
                       meshy.connectPeer(new InetSocketAddress(entry.getKey(), entry.getValue()));
                }
            }
        } else if (meshPeers != null) {
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

    protected void writeQuery(ChannelHandlerContext ctx, Query query, ChannelPromise promise) throws Exception {
        String[] opsLog = query.getOps();   // being able to log and monitor rops is kind of important

        // resolves alias, checks querying enabled (!mutates query!)
        if (spawnDataStoreHandler != null) {
            spawnDataStoreHandler.resolveAlias(query);
            spawnDataStoreHandler.validateJobForQuery(query);
        }

        // creates query for worker and updates local query ops (!mutates query!)
        // TODO: fix this pipeline interface
        Query remoteQuery = query.createPipelinedQuery();

        Multimap<Integer, FileReference> fileReferenceMap;
        try {
            fileReferenceMap = cachey.get(query.getJob());
            if ((fileReferenceMap == null) || fileReferenceMap.isEmpty()) {
                cachey.invalidate(query.getJob());
                throw new QueryException("[MeshQueryMaster] No file references found for job: " + query.getJob());
            }
        } catch (ExecutionException e) {
            log.warn("", e);
            throw new QueryException("Exception getting file references: " + e.getMessage());
        }

        int canonicalTasks;
        if (spawnDataStoreHandler != null) {
            try {
                canonicalTasks = spawnDataStoreHandler.validateTaskCount(query, fileReferenceMap);
            } catch (Exception ex) {
                cachey.invalidate(query.getJob());
                throw ex;
            }
        } else {
            // task-ids are zero indexed, so add one
            canonicalTasks = fileReferenceMap.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
        }

        QueryTaskSource[] sourcesByTaskID = new QueryTaskSource[canonicalTasks];
        for (int i = 0; i < canonicalTasks; i++) {
            Collection<FileReference> sourceOptions = fileReferenceMap.get(i);
            if (sourceOptions != null) {
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

        MeshSourceAggregator aggregator = new MeshSourceAggregator(sourcesByTaskID, meshy, this, remoteQuery);
        ctx.pipeline().addLast(ctx.executor(), "query aggregator", aggregator);
        TrackerHandler trackerHandler = new TrackerHandler(tracker, opsLog);
        ctx.pipeline().addLast(ctx.executor(), "query tracker", trackerHandler);
        ctx.pipeline().remove(this);
        ctx.pipeline().write(query, promise);
    }

    /**
     * Called after MeshSourceAggregator detects that one of the FileReferences in the cache is invalid/out of date.
     * Look for an alternate FileReferenceWrapper in the cache. If none exists, fetch a replacement via a mesh lookup.
     *
     * @param job             The job id to search
     * @param task            The task id to search
     * @param failedReference The FileReference that threw the exception
     * @return A replacement FileReference, which is also placed into the cache if it was newly generated
     * @throws IOException If there is a problem fetching a replacement FileReference
     */
    public QueryTaskSourceOption getReplacementQueryTaskOption(String job, int task, FileReference failedReference)
            throws IOException, ExecutionException, InterruptedException {
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
}

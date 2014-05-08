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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.query.aggregate.AggregateConfig;
import com.addthis.hydra.query.aggregate.MeshSourceAggregator;
import com.addthis.hydra.query.aggregate.QueryTaskSource;
import com.addthis.hydra.query.aggregate.QueryTaskSourceOption;
import com.addthis.hydra.query.tracker.QueryTracker;
import com.addthis.hydra.query.tracker.TrackerHandler;
import com.addthis.hydra.query.web.HttpUtils;
import com.addthis.hydra.query.zookeeper.ZookeeperHandler;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

@ChannelHandler.Sharable
public class MeshQueryMaster extends SimpleChannelInboundHandler<Query> {

    private static final Logger log = LoggerFactory.getLogger(MeshQueryMaster.class);
    private static final String tempDir = Parameter.value("query.tmpdir", "query.tmpdir");
    private static final int meshPort = Parameter.intValue("qmaster.mesh.port", 5100);
    private static final String meshRoot = Parameter.value("qmaster.mesh.root", "/home/hydra");
    private static final String meshPeers = Parameter.value("qmaster.mesh.peers", "localhost");
    private static final int meshPeerPort = Parameter.intValue("qmaster.mesh.peer.port", 5101);
    private static final boolean enableZooKeeper = Parameter.boolValue("qmaster.enableZooKeeper", true);

    /**
     * used for tracking metrics and other interesting things about queries
     * that we have run.  Provides insight into currently running queries
     * and gives ability to cancel a query before it completes.
     */
    private final QueryTracker tracker;

    /**
     * Primary Mesh server
     */
    private final MeshyServer meshy;

    /**
     * Abstracts away zookeeper reliant functions
     */
    private final ZookeeperHandler keepy;

    /**
     * Mesh FileRef Cache -- backed by a loading cache
     */
    private final MeshFileRefCache cachey;

    /**
     * The executor group that queries are run in.
     */
    public final EventExecutorGroup executorGroup =
            new DefaultEventExecutorGroup(AggregateConfig.FRAME_READER_THREADS);

    public MeshQueryMaster(QueryTracker tracker) throws Exception {
        this.tracker = tracker;

        meshy = new MeshyServer(meshPort, new File(meshRoot));
        cachey = new MeshFileRefCache(meshy);
        connectToMeshPeers();

        try {
            // Delete the tmp directory (disk sort directory)
            File tempDirFile = new File(tempDir).getCanonicalFile();
            Files.deleteDir(tempDirFile);
            Files.initDirectory(tempDirFile);
        } catch (Exception e) {
            log.warn("Error while cleaning / locating the temp directory (for disk sorts).", e);
        }

        if (enableZooKeeper) {
            keepy = new ZookeeperHandler();
        } else {
            keepy = null;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    public ZookeeperHandler keepy() {
        return keepy;
    }

    public QueryTracker getQueryTracker() {
        return tracker;
    }

    protected void shutdown() {
        try {
            if (keepy != null) {
                keepy.close();
            }
            meshy.close();
            executorGroup.shutdownGracefully().sync();
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
    protected void channelRead0(ChannelHandlerContext ctx, Query msg) throws Exception {
        messageReceived(ctx, msg); // redirect to more sensible netty5 naming scheme
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("Exception caught while serving http query endpoint", cause);
        if (ctx.channel().isActive()) {
            HttpUtils.sendError(ctx, new HttpResponseStatus(500, cause.getMessage()));
        }
    }

    protected void messageReceived(ChannelHandlerContext ctx, Query query) throws Exception {
        String[] opsLog = query.getOps();   // being able to log and monitor rops is kind of important

        // creates query for worker and updates local query ops (!mutates query!)
        // TODO: fix this pipeline interface
        Query remoteQuery = query.createPipelinedQuery();

        if (keepy != null) {
            keepy.resolveAlias(query);
            keepy.validateJobForQuery(query);
        }

        Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap;
        try {
            fileReferenceMap = cachey.get(query.getJob());
            if ((fileReferenceMap == null) || fileReferenceMap.isEmpty()) {
                throw new QueryException("[MeshQueryMaster] No file references found for job: " + query.getJob());
            }
        } catch (ExecutionException e) {
            log.warn("", e);
            throw new QueryException("Exception getting file references: " + e.getMessage());
        }

        int canonicalTasks = 0;

        if (keepy != null) {
            canonicalTasks = keepy.validateTaskCount(query, fileReferenceMap);
        } else {
            for (Integer taskId : fileReferenceMap.keySet()) {
                if (taskId > canonicalTasks) {
                    canonicalTasks = taskId;
                }
            }
            // tasks are zero indexed
            canonicalTasks += 1;
        }

        QueryTaskSource[] sourcesByTaskID = new QueryTaskSource[canonicalTasks];
        for (int i = 0; i < canonicalTasks; i++) {
            Set<FileReferenceWrapper> sourceOptions = fileReferenceMap.get(i);
            QueryTaskSourceOption[] taskSourceOptions = new QueryTaskSourceOption[sourceOptions.size()];
            int taskSourceOptionsIndex = 0;
            for (FileReferenceWrapper wrapper : sourceOptions) {
                FileReference queryReference = wrapper.fileReference;
                taskSourceOptions[taskSourceOptionsIndex] = new QueryTaskSourceOption(queryReference);
                taskSourceOptionsIndex += 1;
            }
            sourcesByTaskID[i] = new QueryTaskSource(taskSourceOptions);
        }

        MeshSourceAggregator aggregator = new MeshSourceAggregator(sourcesByTaskID, meshy, this, remoteQuery);
        ctx.pipeline().addLast(executorGroup, "query aggregator", aggregator);
        TrackerHandler trackerHandler = new TrackerHandler(tracker, opsLog);
        ctx.pipeline().addLast(executorGroup, "query tracker", trackerHandler);
        ctx.fireChannelRead(query);
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
    public FileReference getReplacementFileReferenceForSingleTask(String job, int task, FileReference failedReference) throws IOException {
        Set<FileReferenceWrapper> wrappers = cachey.getTaskReferencesIfPresent(job, task);
        if (wrappers != null) {
            wrappers.remove(new FileReferenceWrapper(failedReference, task));
            if (!wrappers.isEmpty()) {
                cachey.updateFileReferenceForTask(job, task, new HashSet<>(wrappers));
                return wrappers.iterator().next().fileReference;
            }
        }
        // If mqm got to this point, there was no replacement fileReference in the cache, so we need to fetch a new one
        FileReference freshFileReference = cachey.getFileReferenceWrapperForSingleTask(job, task).fileReference;
        HashSet<FileReferenceWrapper> baseSet = wrappers == null ? new HashSet<FileReferenceWrapper>() : new HashSet<>(wrappers);
        baseSet.add(new FileReferenceWrapper(freshFileReference, task));
        cachey.updateFileReferenceForTask(job, task, baseSet);
        return freshFileReference;
    }
}

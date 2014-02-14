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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.data.query.source.ErrorHandlingQuerySource;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.query.util.MeshSourceAggregator;
import com.addthis.hydra.query.util.QueryData;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MeshQueryMaster implements ErrorHandlingQuerySource {

    private static final Logger log = LoggerFactory.getLogger(MeshQueryMaster.class);
    private static final String tempDir = Parameter.value("query.tmpdir", "query.tmpdir");
    private static final int failureExpireMinutes = Parameter.intValue("qmaster.failureExpireMinutes", 5);
    private static final int meshPort = Parameter.intValue("qmaster.mesh.port", 5100);
    private static final String meshRoot = Parameter.value("qmaster.mesh.root", "/home/hydra");
    private static final String meshPeers = Parameter.value("qmaster.mesh.peers", "localhost");
    private static final int meshPeerPort = Parameter.intValue("qmaster.mesh.peer.port", 5101);
    private static final boolean enableZooKeeper = Parameter.boolValue("qmaster.enableZooKeeper", true);

    /**
     * A ZooKeeper/Priam backed data structure that keeps track of
     * the Hydra jobs in the cluster.  We are specifically
     * interested in how many tasks are in each job so we know
     * the minimum number of responses required to run a query.
     */
    private final JobConfigManager jobConfigManager;

    /**
     * A SpawnDataStore used by the JobConfigManager to fetch job status.
     * Uses either zookeeper or priam depending on the cluster config.
     */
    private final SpawnDataStore spawnDataStore;

    /**
     * Monitors ZooKeeper for query configuration information.
     * Used to determine if queries are enabled and/or traceable
     * for a given job
     */
    private final QueryConfigWatcher queryConfigWatcher;

    /**
     * A ZooKeeper backed data structure that maintains a
     * bi-directional mapping of job aliases to job IDs
     */
    private final AliasBiMap aliasBiMap;

    /**
     * Used to determine if a job has failed.  In the case
     * where a single alias is mapped to multiple jobs
     * this can be useful to fail over to another version of a
     * job when the primary job is not reachable
     */
    private final JobFailureDetector jobFailureDetector;

    /**
     * used for tracking metrics and other interesting things about queries
     * that we have run.  Provides insight into currently running queries
     * and gives ability to cancel a query before it completes.
     */
    private final QueryTracker tracker;

    /**
     * used for tracking metrics and information regarding query worker mesh hosts
     */
    private final MeshHostTracker hostTracker;

    /**
     * Primary Mesh server
     */
    private final MeshyServer meshy;

    /**
     * Mesh FileRef Cache -- backed by a loading cache
     */
    private final MeshFileRefCache cachey;

    /**
     * a cache of job configuration data, used to reduce load placed on ZK server with high volume queries
     */
    private final LoadingCache<String, IJob> jobConfigurationCache = CacheBuilder.newBuilder()
            .maximumSize(2000)
            .refreshAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, IJob>() {
                        public IJob load(String jobId) throws IOException {
                            return jobConfigManager.getJob(jobId);
                        }
                    });

    private final ConcurrentHashMap<String, Boolean> hostMap = new ConcurrentHashMap<>();

    public MeshQueryMaster(QueryTracker tracker) throws Exception {
        this.tracker = tracker;

        meshy = new MeshyServer(meshPort, new File(meshRoot));
        cachey = new MeshFileRefCache(meshy);
        connectToMeshPeers();
        this.hostTracker = new MeshHostTracker(meshy);

        try {
            // Delete the tmp directory (disk sort directory)
            File tempDirFile = new File(tempDir).getCanonicalFile();
            Files.deleteDir(tempDirFile);
            Files.initDirectory(tempDirFile);
        } catch (Exception e) {
            log.warn("Error while cleaning / locating the temp directory (for disk sorts).", e);
            }

        if (enableZooKeeper) {
            spawnDataStore = DataStoreUtil.makeSpawnDataStore();
            this.jobConfigManager = new JobConfigManager(spawnDataStore);
            this.jobFailureDetector = new JobFailureDetector(failureExpireMinutes);
            this.queryConfigWatcher = new QueryConfigWatcher(spawnDataStore);
            this.aliasBiMap = new AliasBiMap(spawnDataStore);
            this.aliasBiMap.loadCurrentValues();
        } else {
            this.spawnDataStore = null;
            this.jobConfigManager = null;
            this.jobFailureDetector = null;
            this.queryConfigWatcher = null;
            this.aliasBiMap = null;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    public QueryTracker getQueryTracker() {
        return tracker;
    }

    protected void shutdown() {
        try {
            if (spawnDataStore != null) {
                spawnDataStore.close();
            }
            meshy.close();
        } catch (Exception e) {
            e.printStackTrace();
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

    public JSONArray getHostsInfo() throws JSONException {
        JSONArray hosts = new JSONArray();
        MeshyServer[] meshyServers = meshy.getMembers();
        Collection<ChannelState> channels = meshy.getChannels(null);
        for (ChannelState channelState : channels) {
            InetSocketAddress address = channelState.getRemoteAddress();
            JSONObject host = new JSONObject();
            host.put("name", channelState.getName());
            host.put("host", address.getHostName());
            host.put("port", address.getPort());
            hosts.put(host);
        }
        return hosts;
    }

    @Override
    public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException {
        try {
            /* creates query for worker and updates local query ops */
            Query remoteQuery = query.createPipelinedQuery();

            String job = query.getJob();
            if (enableZooKeeper && aliasBiMap == null) {
                throw new QueryException("QueryMaster has not been initialized, try query again soon...");
            }

            job = validateJobForQuery(query, job);

            query.setTraced((enableZooKeeper && queryConfigWatcher.shouldTrace(query.getJob())) || query.isTraced());

            /** create a processor chain based in query ops terminating in provided consumer */
            consumer = query.getProcessor(consumer, new QueryStatusObserver());

            Map<Integer, Set<FileReferenceWrapper>> fileReferenceMap;
            try {
                fileReferenceMap = cachey.get(query.getJob());
                if (fileReferenceMap == null || fileReferenceMap.size() == 0) {
                    throw new QueryException("[MeshQueryMaster] No file references found for job: " + query.getJob());
                }
            } catch (ExecutionException e)  {
                log.warn("", e);
                throw new QueryException("Exception getting file references: " + e.getMessage());
            }

            if (enableZooKeeper) {
                IJob zkJob;
                try {
                    zkJob = jobConfigurationCache.get(job);
                } catch (ExecutionException e) {
                    throw new QueryException("unable to retrieve job configuration for job: " + job);
                }
                if (zkJob == null) {
                    final String errorMessage = "[MeshQueryMaster] Error:  unable to find ZK reference for job: " + job;
                    throw new QueryException(errorMessage);
                }

                final int taskCount = new Job(zkJob).getTaskCount();
                int fileReferenceCount = fileReferenceMap.size();
                if (!(query.getParameter("allowPartial") != null && Boolean.valueOf(query.getParameter("allowPartial"))) && fileReferenceCount != taskCount) {
                    final String errorMessage = "Did not find data for all tasks (and allowPartial is off): " + fileReferenceCount + " out of " + taskCount;
                    final int numMissing = taskCount - fileReferenceCount;
                    final String label = "\n Missing the following " + numMissing + " tasks : ";
                    final StringBuilder sb = new StringBuilder();
                    final TreeMap<Integer, Set<FileReferenceWrapper>> sortedMap = new TreeMap<>(fileReferenceMap);
                    final Iterator<Integer> it = sortedMap.keySet().iterator();
                    Integer key = it.next();
                    for (int i = 0; i < taskCount; i++) {
                        if (key == null || i != key) {
                            if (sb.length() > 0) {
                                sb.append(", ");
                            }
                            sb.append(i);
                        } else {
                            key = it.hasNext() ? it.next() : null;
                        }
                    }
                    throw new QueryException(errorMessage + label + sb.toString());
                }
            }

            final Map<Integer, Set<QueryData>> sourceMap = new HashMap<>();
            final HashMap<String, String> options = new HashMap<>();
            options.put("query", CodecJSON.encodeString(remoteQuery));
            Set<QueryData> potentialQueryDataList = new HashSet<>();
            for (Map.Entry<Integer, Set<FileReferenceWrapper>> entry : fileReferenceMap.entrySet()) {
                HashSet<QueryData> queryDataSet = new HashSet<>();
                for (FileReferenceWrapper wrapper : entry.getValue()) {
                    QueryData queryData = new QueryData(meshy, wrapper.fileReference, options, query.getJob(), wrapper.taskId);
                    queryDataSet.add(queryData);
                    potentialQueryDataList.add(queryData);
                }
                sourceMap.put(entry.getKey(), queryDataSet);
            }
            MeshSourceAggregator aggregator = new MeshSourceAggregator(sourceMap, new ConcurrentHashMap<>(hostMap), this);
            QueryHandle handle = tracker.runAndTrackQuery(aggregator, potentialQueryDataList, query, consumer);
            if (enableZooKeeper) {
                jobFailureDetector.indicateSuccess(query.getParameter("job", query.getJob()));
            }
            return handle;
        } catch (QueryException qe) {
            consumer.sourceError(qe);
            throw qe;
        }
    }

    private String validateJobForQuery(Query query, String job) {
        if (!enableZooKeeper) {
            return job;
        }
        List<String> possibleJobs = aliasBiMap.getJobs(job);
        if (possibleJobs != null && possibleJobs.size() > 0) {
            for (String pjob : possibleJobs) {
                int threshold = queryConfigWatcher.consecutiveFailureThreshold(pjob);
                if (!jobFailureDetector.hasFailed(pjob, threshold > 0 ? threshold : Integer.MAX_VALUE)) {
                    query.setParameter("track.alias", job);
                    query.setJob(pjob);
                    return pjob;
                }
            }
            // if we get here it means we've not found a good job
            throw new QueryException("All available jobs for " + job + " have failed :" + possibleJobs.toString());
        } else {
            String trackA = aliasBiMap.getLikelyAlias(job);
            query.setParameter("track.alias", trackA != null ? trackA : "");
        }
        if (!queryConfigWatcher.jobIsTracked(query.getJob())) {
            throw new QueryException("job not found (wrong job id or not detected yet): " + job);
        }
        if (!queryConfigWatcher.safeToQuery(query.getJob())) {
            throw new QueryException("job is not safe to query (are queries enabled for this job in spawn?): " + job);
        }
        return job;
    }

    public synchronized void invalidateFileReferenceForJob(String job) {
        if (job != null) {
            cachey.invalidate(job);
        }
    }

    public synchronized void invalidateFileReferenceCache() {
        cachey.invalidateFileReferenceCache();
    }

    public FileReference getFileReferenceForSingleTask(String job, int task) throws IOException {
        return cachey.getFileReferenceWrapperForSingleTask(job, task).fileReference;
    }

    public String getMeshHostJSON() {
        return this.hostTracker.getHostsJSON();
    }

    public Collection<IJob> getJobs() {
        return jobConfigManager.getJobs().values();
    }

    @Override
    public void noop() {
    }

    @Override
    public void handleError(Query query) {
        cachey.invalidate(query.getJob());
    }

    @Override
    public boolean isClosed() {
        // if we are running we are not closed
        return false;
    }
}

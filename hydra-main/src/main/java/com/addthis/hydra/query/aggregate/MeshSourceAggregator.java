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
package com.addthis.hydra.query.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.hydra.query.util.QueryData;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeshSourceAggregator implements QuerySource {

    static final Logger log = LoggerFactory.getLogger(MeshSourceAggregator.class);

    static final int frameReaderThreads = Parameter.intValue("meshSourceAggregator.frameReaderThreads", 50);
    static final int queryOpThreads = Parameter.intValue("meshSourceAggregator.queryOpThreads", 50);

    static final boolean parallelQuery = Parameter.boolValue("qmaster.parallelQuery", false);

    static final ExecutorService frameReaderPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(frameReaderThreads, frameReaderThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("frameReader-%d").build()));

    static final ExecutorService queryOpPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(queryOpThreads, queryOpThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("queryOp-%d").build()));


    // Queue for sources -- one source per task per query
    static final BlockingQueue<QueryTaskSource> readerQueue = new LinkedBlockingDeque<>();

    static final boolean enableStragglerCheck = Parameter.boolValue("meshSourceAggregator.enableStragglerCheck", false);
    // Enables straggler check logic.
    static final int stragglerCheckPeriod = Parameter.intValue("meshSourceAggregator.stragglerCheckPeriodMillis", 1000);
    // Every 100 ms, check for stragglers
    static final double stragglerCheckHostPercentage = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckHostPercentage", ".2"));
    // alternate straggler method
    static final boolean useStdDevStragglers = Parameter.boolValue("meshSourceAggregator.useStdDevStraggles", false);
    // A task could be a straggler if it's in the last 10% of tasks to return
    static final double stragglerCheckMeanRuntimeFactor = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckMeanRuntimeFactor", "1.3"));
    // A task actually is a straggler if its runtime is more than 1.5 times the mean runtime of tasks for this query
    static final boolean prioritiseReadOnlyWorkers = Parameter.boolValue("meshSourceAggregator.prioritiseReadOnlyWorkers", false);
    static final int pollWaitTime = Parameter.intValue("meshSourceAggregator.pollWaitTime", 50);

    /**
     * Identifies the number of standard deviations required to run stragglers when hosts are slow
     */
    static final double MULTIPLE_STD_DEVS = Double.parseDouble(Parameter.value("meshSourceAggregator.multipleStdDevs", "2"));

    static final AtomicBoolean initialize = new AtomicBoolean(false);
    static final AtomicBoolean exiting = new AtomicBoolean(false);

    /* metrics */
    static Counter totalQueries = Metrics.newCounter(MeshSourceAggregator.class, "totalQueries");
    static Counter totalStragglerCheckerRequests = Metrics.newCounter(MeshSourceAggregator.class, "totalStragglerCheckerRequests");
    static Counter totalRetryRequests = Metrics.newCounter(MeshSourceAggregator.class, "totalRetryRequests");

    final Map<Integer, Set<QueryData>> sourcesByTaskID;
    final Map<String, Boolean> hostMap;
    final ConcurrentHashMap<Integer, String> queryResponders = new ConcurrentHashMap<Integer, String>();
    final int totalTasks;
    final long startTime;
    final Lock runtimeLock = new ReentrantLock();
    AggregateHandle handle;
    static StragglerCheckThread stragglerCheckThread;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                exiting.set(true);
            }
        });
    }

    public MeshSourceAggregator(Map<Integer, Set<QueryData>> sourcesByTaskID, Map<String, Boolean> hostMap, MeshQueryMaster meshQueryMaster) {
        this.sourcesByTaskID = sourcesByTaskID;
        this.hostMap = hostMap;
        totalTasks = sourcesByTaskID.size();
        if (initialize.compareAndSet(false, true)) {
            for (int i = 0; i < frameReaderThreads; i++) {
                frameReaderPool.submit(new SourceReader(meshQueryMaster));
            }

            if (enableStragglerCheck) {
                stragglerCheckThread = new StragglerCheckThread();
            }
        }
        this.startTime = JitterClock.globalTime();
    }

    @Override
    public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException {
        try {
            final String queryParallel = query.getParameter("parallel");
            final boolean useParallel = (queryParallel == null ? parallelQuery : Boolean.valueOf(queryParallel));
            synchronized (sourcesByTaskID) {
                if (log.isDebugEnabled() || query.isTraced()) {
                    Query.emitTrace("Query: " + query.uuid() + " will " + (useParallel ? " be " : " not be ") + " executed in parallel mode and sent to a total of " + sourcesByTaskID.size() + " sources");
                }
                totalQueries.inc();
                handle = new AggregateHandle(this, query, consumer, sourcesByTaskID);
                Map<String, Integer> taskPerHostCount = new HashMap<>();
                for (Map.Entry<Integer, Set<QueryData>> entry : sourcesByTaskID.entrySet()) {
                    if (useParallel) {
                        for (QueryData queryData : entry.getValue()) {
                            requestQueryData(queryData, query);
                        }
                    } else {
                        QueryData chosenQueryData = allocateQueryTaskLegacy(taskPerHostCount, entry.getValue(), hostMap);
                        entry.getValue().remove(chosenQueryData);
                        requestQueryData(chosenQueryData, query);
                    }
                }
            }
            if (!useParallel && enableStragglerCheck && stragglerCheckThread != null) {
                stragglerCheckThread.addQuery(query, handle, this);
            }
        } catch (Exception e) {
            throw new QueryException("Exception submitting queries", e);
        }
        return handle;
    }

    String requestQueryData(QueryData queryData, Query query) {
        QueryTaskSource reader = new QueryTaskSource(this, queryData, handle, query);
        readerQueue.add(reader);
        if (log.isTraceEnabled()) {
            log.trace("Setting start time. QueryID:" + query.uuid() + " host:" + queryData.hostEntryInfo.getHostName());
        }
        return reader.id;
    }

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }


    /**
     * Allocate the query task to the best available host.  Currently we only try
     * to evenly distribute the queries amongst available hosts.  For example if hosts
     * 1 and 2 both have data for task a but host 1 already has a query task assigned
     * to it for a different task then we will pick host 2.
     *
     * @param queryPerHostCountMap - map of number of queries assigned to each host
     * @param queryDataSet         - the available queryData objects for the task being assigned
     * @param hostMap              - the map of host ids to a boolean describing whether the host is read only.
     */
    public static QueryData allocateQueryTaskLegacy(Map<String, Integer> queryPerHostCountMap,
            Set<QueryData> queryDataSet, Map<String, Boolean> hostMap) {
        if (queryDataSet == null || queryDataSet.size() == 0) {
            throw new RuntimeException("fileReferenceWrapper list cannot be empty");
        }
        SortedSet<QueryData> hostList = new TreeSet<>();
        int taskId = -1;
        for (QueryData queryData : queryDataSet) {
            taskId = queryData.taskId;
            String host = queryData.hostEntryInfo.getHostName();
            // initialize map
            if (queryPerHostCountMap.get(host) == null) {
                queryPerHostCountMap.put(host, 0);
            }
            hostList.add(queryData);
        }

        if (log.isTraceEnabled()) {
            log.trace("hostList size for task: " + taskId + " was " + hostList.size());
        }

        int minNumberAssigned = -1;
        QueryData bestQueryData = null;
        boolean readOnlyHostSelected = false;
        for (QueryData queryData : hostList) {
            String host = queryData.hostEntryInfo.getHostName();
            int numberAssigned = queryPerHostCountMap.get(host);
            if (log.isTraceEnabled()) {
                log.trace("host: " + host + " currently has: " + numberAssigned + " assigned");
            }
            boolean readOnlyHost = hostMap.containsKey(host) && hostMap.get(host);
            if (log.isTraceEnabled()) {
                log.trace(host + " readOnly:" + readOnlyHost);
            }
            // if we haven't picked any host
            // or if the host we are evaluating is a readOnly host
            // or if the current number of sub queries assigned to this host is less than the least loaded host we have found so far
            if (minNumberAssigned < 0 || (readOnlyHost && prioritiseReadOnlyWorkers) || numberAssigned < minNumberAssigned) {
                // we only update the 'best' variable if one of two conditions is met:
                // 1 - we have not already selected a read only host
                // 2- we have selected a read only host AND the new host is read only and it has fewer sub-queries assigned to it
                if (!readOnlyHostSelected || (readOnlyHost && numberAssigned < minNumberAssigned)) {
                    minNumberAssigned = numberAssigned;
                    bestQueryData = queryData;
                    if (readOnlyHost) {
                        readOnlyHostSelected = true;
                    }
                }
            }
        }
        // bestWrapper should never be null here
        if (bestQueryData == null) {
            throw new QueryException("Unable to select best query data");
        }
        if (log.isTraceEnabled()) {
            log.trace("selected host: " + bestQueryData.hostEntryInfo.getHostName() + " as best host for task: " + bestQueryData.taskId);
        }
        queryPerHostCountMap.put(bestQueryData.hostEntryInfo.getHostName(), (queryPerHostCountMap.get(bestQueryData.hostEntryInfo.getHostName()) + 1));
        return bestQueryData;
    }


}

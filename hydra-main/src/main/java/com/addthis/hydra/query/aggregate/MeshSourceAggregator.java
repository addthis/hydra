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
import com.addthis.hydra.query.web.DataChannelOutputToNettyBridge;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MeshSourceAggregator extends SimpleChannelInboundHandler<Query> implements QuerySource {

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
    protected void channelRead0(ChannelHandlerContext ctx, Query msg) throws Exception {
        messageReceived(ctx, msg); // redirect to more sensible netty5 naming scheme
    }

    protected void messageReceived(ChannelHandlerContext ctx, Query msg) throws Exception {
        DataChannelOutput consumer = new DataChannelOutputToNettyBridge(ctx);
        try {
            query(msg, consumer); // ignore query handle
        } catch (Exception ex) {
//            handleError(msg);
            consumer.sourceError(new QueryException(ex));
        }
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
                        QueryData chosenQueryData = TaskAllocator.allocateQueryTaskLegacy(taskPerHostCount, entry.getValue(), hostMap);
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
}

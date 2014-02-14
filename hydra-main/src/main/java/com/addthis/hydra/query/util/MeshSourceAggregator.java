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
package com.addthis.hydra.query.util;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryConsumer;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.query.MeshQueryMaster;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MeshSourceAggregator implements com.addthis.hydra.data.query.source.QuerySource {

    private static final Logger log = LoggerFactory.getLogger(MeshSourceAggregator.class);

    private static final int frameReaderThreads = Parameter.intValue("meshSourceAggregator.frameReaderThreads", 50);
    private static final int queryOpThreads = Parameter.intValue("meshSourceAggregator.queryOpThreads", 50);

    private static final boolean parallelQuery = Parameter.boolValue("qmaster.parallelQuery", false);

    private static final ExecutorService frameReaderPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(frameReaderThreads, frameReaderThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("frameReader-%d").build()));

    private static final ExecutorService queryOpPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(queryOpThreads, queryOpThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("queryOp-%d").build()));


    // Queue for sources -- one source per task per query
    private static final BlockingQueue<QuerySource> readerQueue = new LinkedBlockingDeque<>();

    private static final boolean enableStragglerCheck = Parameter.boolValue("meshSourceAggregator.enableStragglerCheck", false);
    // Enables straggler check logic.
    private static final int stragglerCheckPeriod = Parameter.intValue("meshSourceAggregator.stragglerCheckPeriodMillis", 1000);
    // Every 100 ms, check for stragglers
    private static final double stragglerCheckHostPercentage = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckHostPercentage", ".2"));
    // alternate straggler method
    private static final boolean useStdDevStragglers = Parameter.boolValue("meshSourceAggregator.useStdDevStraggles", false);
    // A task could be a straggler if it's in the last 10% of tasks to return
    private static final double stragglerCheckMeanRuntimeFactor = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckMeanRuntimeFactor", "1.3"));
    // A task actually is a straggler if its runtime is more than 1.5 times the mean runtime of tasks for this query
    private static final boolean prioritiseReadOnlyWorkers = Parameter.boolValue("meshSourceAggregator.prioritiseReadOnlyWorkers", false);
    private static final int pollWaitTime = Parameter.intValue("meshSourceAggregator.pollWaitTime", 50);

    /**
     * Identifies the number of standard deviations required to run stragglers when hosts are slow
     */
    private static final double MULTIPLE_STD_DEVS = Double.parseDouble(Parameter.value("meshSourceAggregator.multipleStdDevs", "2"));

    /* A cache for task ids that recently were not in their expected locations */
    private static final int RECENTLY_MISPLACED_TASK_TTL_MILLIS = Parameter.intValue("meshSourceAggregator.recentlyMisplacedTaskTtl", 120000);
    private static final int RECENTLY_MISPLACED_TASK_MAX_SIZE = Parameter.intValue("meshSourceAggregator.recentlyMisplacedTaskTtl", 300);
    private static final Cache<String, Boolean> recentMisplacedTasks;

    private static final AtomicBoolean initialize = new AtomicBoolean(false);
    private static final AtomicBoolean exiting = new AtomicBoolean(false);

    /* metrics */
    private static Counter totalQueries = Metrics.newCounter(MeshSourceAggregator.class, "totalQueries");
    private static Counter totalStragglerCheckerRequests = Metrics.newCounter(MeshSourceAggregator.class, "totalStragglerCheckerRequests");

    private final Map<Integer, Set<QueryData>> sourcesByTaskID;
    private final Map<String, Boolean> hostMap;
    private final ConcurrentHashMap<Integer, String> queryResponders = new ConcurrentHashMap<Integer, String>();
    private final int totalTasks;
    private final long startTime;
    private final Lock runtimeLock = new ReentrantLock();
    private AggregateHandle handle;
    private static StragglerCheckThread stragglerCheckThread;

    static {
        recentMisplacedTasks = CacheBuilder.newBuilder().maximumSize(RECENTLY_MISPLACED_TASK_MAX_SIZE)
                .expireAfterWrite(RECENTLY_MISPLACED_TASK_TTL_MILLIS, TimeUnit.MILLISECONDS).build();
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
                handle = new AggregateHandle(query, consumer, sourcesByTaskID);
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

    private String requestQueryData(QueryData queryData, Query query) {
        QuerySource reader = new QuerySource(queryData, handle, query);
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


    private class AggregateHandle implements QueryHandle, DataChannelOutput {

        //handles MUST by synchronized on
        private final List<QuerySource> handles;
        private final QueryConsumer consumer;
        private final AtomicInteger counter;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final Query query;
        private volatile DataChannelError error;
        private final AtomicBoolean errored = new AtomicBoolean(false);
        private final AtomicBoolean canceled = new AtomicBoolean(false);
        private final Set<Integer> completed;
        private final Set<Integer> started;
        private final List<Long> runtimes;
        private final int _totalTasks;
        private final Map<Integer, Set<QueryData>> sourcesByTaskID;

        AggregateHandle(final Query query, final DataChannelOutput consumer, Map<Integer, Set<QueryData>> sourcesByTaskID) {
            this.query = query;
            this.consumer = new QueryConsumer(consumer);
            this._totalTasks = totalTasks;
            this.counter = new AtomicInteger(0);
            this.handles = new ArrayList<>(totalTasks);
            this.completed = Collections.synchronizedSet(new HashSet<Integer>(totalTasks));
            this.started = Collections.synchronizedSet(new HashSet<Integer>(totalTasks));
            this.runtimes = new ArrayList<>(totalTasks);
            this.sourcesByTaskID = sourcesByTaskID;
        }

        public long getStartTime() {
            return startTime;
        }

        public void addHandle(QuerySource handle) {
            synchronized (handles) {
                if (!canceled.get()) {
                    this.handles.add(handle);
                } else {
                    //does not add handle to handles to prevent a possible double cancel.
                    // currently handles aren't used anywhere else so this is fine, otherwise
                    // an extra flag or something can be easily added
                    cancelHandle(handle, "Handle added after query was canceled");
                }
            }
        }

        @Override
        public void cancel(String message) //closes connections to workers and consumer
        {
            log.warn("[aggregate.handle] cancel called.  canceled:" + canceled.get() + " erred:" + errored.get());
            // only cancel one time here
            if (canceled.compareAndSet(false, true)) {

                if (log.isTraceEnabled()) {
                    log.trace("CANCEL called: Deleting metrics for query: " + query.uuid());
                }

                //Close connections to workers and call done if needed
                synchronized (handles) {
                    for (QueryHandle handle : handles) {
                        cancelHandle(handle, message);
                    }
                }
            }
        }

        private void cancelHandle(QueryHandle handle, String message) {
            try {
                handle.cancel(message); //QuerySource usually
            } catch (Exception ex) {
                String msg = String.format("error cancelling query %s # %d message: %s",
                        query.uuid(), counter.incrementAndGet(), message);
                log.warn(msg, ex);
            }
        }

        @Override
        public void send(Bundle bundle) throws DataChannelError {
            try {
                consumer.send(bundle);
            } catch (DataChannelError e) {
                throw error(e);
            } catch (Exception e) {
                throw error(DataChannelError.promote(e));
            }
        }

        public boolean markTaskStarted(int node) {
            return started.add(node);
        }

        public void markTaskCompleted(int node, QuerySource querySource) {
            long completionTime = JitterClock.globalTime() - startTime;
            runtimeLock.lock();
            try {
                runtimes.add(completionTime);
                if (!completed.add(node) && querySource.lines > 0) {
                    log.warn(query.uuid() + " duplicate complete message for job taskId: " + node + " from sourceId: " + querySource.id + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
                }
            } finally {
                runtimeLock.unlock();
            }
            if (log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace(query.uuid() + " sendComplete for job taskId:" + node + " and pos:" + counter.get() + " from sourceId: " + querySource.queryData.hostEntryInfo.getHostName() + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
            }
            sendComplete();
        }

        @Override
        public void sendComplete() {

            if (completed.size() == totalTasks) {
                done();
            }
        }

        @Override
        public void sourceError(DataChannelError er) {
            error(er);
        }

        @Override
        public Bundle createBundle() {
            return consumer.createBundle();
        }

        @Override
        public void send(List<Bundle> bundles) {
            try {
                consumer.send(bundles);
            } catch (DataChannelError e) {
                throw error(e);
            } catch (Exception e) {
                throw error(DataChannelError.promote(e));
            }
        }

        private DataChannelError error(DataChannelError ex) {
            if (errored.compareAndSet(false, true)) {
                error = ex;
                log.warn(query.uuid() + " error " + ex + " #" + counter.incrementAndGet(), ex);
                cancel(ex.getMessage()); //'usually' does nothing
                done();
            }
            return ex;
        }

        private void done() {
            if (done.compareAndSet(false, true)) {

                if (log.isTraceEnabled()) {
                    log.trace("Committing metrics for query: " + query.uuid() + " and deleting its metrics");
                }

                if (log.isDebugEnabled() || query.isTraced()) {
                    Query.emitTrace(query.uuid() + " done " + consumer + " #" + counter.incrementAndGet());
                }

                if (error != null) {
                    consumer.sourceError(error);
                } else {
                    // this can take a while so run in a thread
                    queryOpPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            consumer.sendComplete();
                        }
                    });
                }
            }
        }

        public boolean isComplete(int node) {
            return completed.contains(node);
        }

        public boolean isStarted(int node) {
            return started.contains(node);
        }

        private double getMeanRuntime() {
            runtimeLock.lock();
            double sum = 0;
            try {
                for (Long runtime : handle.runtimes) {
                    sum += runtime;
                }
                if (handle.runtimes.size() > 0) {
                    sum /= handle.runtimes.size();
                }
            } finally {
                runtimeLock.unlock();
            }
            return sum;
        }

        /**
         * Calculates the point in time multipleStdDevs away from the mean
         *
         * @param multipleStdDevs the number of times to add the stadard deviation
         * @return the mean plus multipleStdDevs of the standard deviation
         */
        private double getStdDevsAwayRuntime(double multipleStdDevs) {
            double x = 0;
            double x2 = 0;
            int n = 0;
            double stdDevsAway = 0;

            runtimeLock.lock();
            try {
                for (Long runtime : runtimes) {
                    x += runtime;
                    x2 += runtime * runtime;
                }
                n = runtimes.size();
            } finally {
                runtimeLock.unlock();
            }

            if (n > 0) {
                double mean = x / n;

                // This is an approximation. With large n we can ignore the -1 in the n-1 in the denominator of the
                // variance. However, to avoid division by 0 we ignore it
                double variance = (x2 - 2 * x * mean + n * mean * mean) / n;
                stdDevsAway = mean + multipleStdDevs * Math.sqrt(variance);
            }

            return stdDevsAway;
        }


        public void handleStragglers() {
            for (int node = 0; node < totalTasks; node++) {
                if (handle.isComplete(node)) {
                    continue;
                }
                if (!handle.isStarted(node)) {
                    synchronized (sourcesByTaskID) {
                        Set<QueryData> queryDataSet = sourcesByTaskID.get(node);
                        if (queryDataSet != null && queryDataSet.size() > 0) {
                            Iterator<QueryData> iterator = queryDataSet.iterator();
                            if (iterator.hasNext()) {
                                QueryData queryData = iterator.next();
                                totalStragglerCheckerRequests.inc();
                                iterator.remove();
                                String id = requestQueryData(queryData, query);
                                if (log.isDebugEnabled() || query.isTraced()) {
                                    Query.emitTrace("Straggler detected for " + query.uuid() + " node " + node + " sending duplicate query to host: " + queryData.hostEntryInfo.getHostName() + " sourceId: " + id);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static class SourceReader implements Runnable {

        private final MeshQueryMaster meshQueryMaster;

        SourceReader (MeshQueryMaster meshQueryMaster) {
            this.meshQueryMaster = meshQueryMaster;
        }

        @SuppressWarnings("InfiniteLoopStatement")
        @Override
        public void run() {
            while (!exiting.get()) {
                QuerySource querySource = null;
                try {
                    querySource = readerQueue.poll(100, TimeUnit.MILLISECONDS);
                    boolean querySourceDone = processQuerySource(querySource);
                    if (!querySourceDone) {
                        // If the query communicated a possibly-recoverable IO failure, get a fresh FileReference and attempt to query it instead
                        String key = querySource.getKey();
                        if (recentMisplacedTasks.getIfPresent(key) == null) {
                            log.warn("Received FileNotFoundException for task " + key + "; attempting retry");
                            processQuerySource(replaceQuerySource(querySource));
                            meshQueryMaster.invalidateFileReferenceForJob(querySource.getJobId());
                        } else {
                            log.warn("Received FileNotFoundException for already-failed task " + key + "; aborting");
                            throw new QueryException("Failed to resolve FileReference for task " + key + " after retry");
                        }
                    }
                } catch (Exception e) {
                    // this is going to rethrow exception, so we need to replace this SourceReader in the pool
                    frameReaderPool.submit(new SourceReader(meshQueryMaster));
                    handleQuerySourceError(querySource, e);
                }
            }
        }

        private QuerySource replaceQuerySource(QuerySource querySource) throws IOException {
            // Invoked when a cached FileReference throws an IO Exception
            // Get a fresh FileReference and make a new QuerySource with that FileReference and the same parameters otherwise
            FileReference fileReference = meshQueryMaster.getFileReferenceForSingleTask(querySource.getJobId(), querySource.getTaskId());
            return querySource.createCloneWithReplacementFileReference(fileReference);
        }

        private boolean processQuerySource(QuerySource querySource) throws DataChannelError, IOException {
            if (querySource != null) {
                // loops until the source is done, query is canceled
                // or source returns a null bundle which indicates its complete or just doesn't
                // have data at the moment
                while (!querySource.done && !querySource.consumer.canceled.get()) {
                    boolean processedNext = false;
                    try {
                        processedNext = processNextBundle(querySource);
                    } catch (DataChannelError err) {
                        log.warn("DCE: " + err.toString() + " :::: " + err.getCause() + " lines=" + querySource.lines);
                        if (querySource.lines == 0) { // && err.getCause instanceof IOException (???)
                            // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                            return false;
                        }
                        else {
                            // This query source has started sending lines. Need to fail the query.
                            throw err;
                        }
                    } catch (IOException io) {
                        log.warn("IO: " + io + " :::: " + io.getCause() + " lines=" + querySource.lines);
                        if (querySource.lines == 0) {
                            // This QuerySource does not have this file anymore. Signal to the caller that a retry may resolve the issue.
                            return false;
                        }
                        else {
                            // This query source has started sending lines. Need to fail the query.
                            throw io;
                        }

                    }

                    if (!processedNext) {
                        // is the source exhausted, not canceled and not obsolete
                        if (querySource.done
                            && !querySource.obsolete
                            && !querySource.consumer.canceled.get()) {
                            // Save the time and lines in the hostEntryInfo
                            QueryData queryData = querySource.queryData;
                            queryData.hostEntryInfo.setLines(querySource.lines);
                            queryData.hostEntryInfo.setFinished();

                            // Mark this task as complete (and query if all done)
                            querySource.consumer.markTaskCompleted(queryData.taskId, querySource);
                            querySource.done = true;
                            querySource.close();

                            if (log.isTraceEnabled()) {
                                log.trace("Adding time & lines: QueryID: " + querySource.query.uuid() + " host:" + queryData.hostEntryInfo.getHostName());
                            }

                        } else if (!querySource.dataChannelReader.busy && querySource.done || querySource.obsolete || querySource.consumer.canceled.get()) {
                            if (log.isTraceEnabled() || querySource.query.isTraced()) {
                                Query.emitTrace("ignoring response for query: " + querySource.query.uuid() + " from source: " + querySource.id + " for task: " + querySource.getTaskId() + " d:" + querySource.done + " o:" + querySource.obsolete + " c:" + querySource.consumer.canceled.get() + " l:" + querySource.lines);
                            }
                            QueryData queryData = querySource.queryData;
                            queryData.hostEntryInfo.setIgnored();
                        }
                        break;
                    }
                }
                if (!querySource.consumer.done.get() && !querySource.consumer.errored.get() &&
                    !querySource.done && !querySource.isEof() && !querySource.obsolete && !querySource.canceled) {
                    // source still has more data but hasn't completed yet.
                    readerQueue.add(querySource);
                }
            }
            return true;
        }

        private void handleQuerySourceError(QuerySource querySource, Exception error) {
            try {
                log.warn("QueryError: " + error.getMessage() + " --- " + querySource.query.uuid() + ":" + querySource.getTaskId() + ":" + querySource.queryData.hostEntryInfo.getHostName() + " failed after receiving: " + querySource.lines + " lines");
                // invalidate file reference cache to prevent persistent errors from bogging everything down
                // TODO:  find more intelligent way to do this...
                meshQueryMaster.invalidateFileReferenceCache();

                // Cancel the query through the query tracker
                meshQueryMaster.getQueryTracker().cancelRunning(querySource.query.uuid(), error.getMessage());

                // Throw the exception so it can be seen in the logs
                if (error instanceof DataChannelError) {
                    throw querySource.consumer.error((DataChannelError) error);
                } else {
                    throw querySource.consumer.error(new DataChannelError(error));
                }
                // TODO: this flat doesn't work, not sure why so removing for now
                // check to see if we should resubmit query
                // resubmitQueryIfPossible(querySource, e);
            } catch (Exception e)  {
                log.warn("", e);
                throw new RuntimeException(e);
            }
        }

        private boolean processNextBundle(QuerySource querySource) throws IOException, DataChannelError {
            Bundle nextBundle = querySource.next();
            if (querySource.canceled || querySource.obsolete) {
                return false;
            }

            if (querySource.query != null && querySource.query.queryStatusObserver != null && querySource.query.queryStatusObserver.queryCompleted) {
                if (log.isDebugEnabled()) {
                    log.debug("Query complete flag is set. Finishing task: " + querySource.getTaskId());
                }
                querySource.done = true;
                return false;
            }

            if (nextBundle != null) {
                querySource.consumer.send(nextBundle);
                return !querySource.isEof();
            }
            return false;
        }
    }

    private class QuerySource implements QueryHandle {

        final QueryData queryData;
        final AggregateHandle consumer;
        final Query query;
        volatile boolean canceled = false;
        volatile boolean obsolete = false;
        volatile boolean done = false;
        private final DataChannelCodec.ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
        private final DataChannelCodec.FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();
        private final SourceInputStream sourceInputStream;
        private FramedDataChannelReader dataChannelReader;
        private int lines;
        private boolean foundBundle = false;
        private AtomicBoolean started = new AtomicBoolean(false);
        private String id = UUID.randomUUID().toString();

        private QuerySource(QueryData queryData, AggregateHandle consumer, Query query) {
            this.queryData = queryData;
            this.consumer = consumer;
            this.query = query;
            this.sourceInputStream = getInputStream();
            dataChannelReader = new FramedDataChannelReader(this.sourceInputStream, queryData.fileReference.name, classMap, fieldMap, pollWaitTime);
            consumer.addHandle(this);
        }

        public SourceInputStream getInputStream() {
            FileReference fileReference = queryData.fileReference;
            StreamSource source = null;
            try {
                source = new StreamSource(queryData.channelMaster, fileReference.getHostUUID(), fileReference.getHostUUID(), fileReference.name, queryData.queryOptions, 0);
            } catch (IOException e) {
                log.warn("Error getting query handle for fileReference: " + fileReference.getHostUUID() + "/" + fileReference.name, e);
                throw new QueryException(e);
            }
            return source.getInputStream();
        }

        private Bundle next() throws IOException, DataChannelError {
            if (canceled || obsolete) {
                // canceled before we got a chance to run;
                return null;
            }

            if (started.compareAndSet(false, true)) {
                queryData.hostEntryInfo.start();
            }

            Bundle bundle;
            if ((bundle = dataChannelReader.read()) != null) {
                // check to see if this is the first response for this node id
                if (!markResponse()) {
                    close();
                    return null;
                }
                consumer.markTaskStarted(queryData.taskId);
                if (canceled) {
                    consumer.sourceError(new DataChannelError("Query Canceled"));
                    return null;
                }
                foundBundle = true;
                lines++;
                queryData.hostEntryInfo.setLines(lines);
            }
            if (isEof()) {
                done = true;
            }
            return bundle;
        }

        public boolean isEof() {
            return dataChannelReader.eof.get();
        }

        private boolean markResponse() {
            final String existingResponse = queryResponders.putIfAbsent(queryData.taskId, id);
            if (existingResponse != null && !existingResponse.equals(id)) {
                if (log.isTraceEnabled() || query.isTraced()) {
                    Query.emitTrace("FileReference: " + queryData.fileReference + " is not the first to respond for task ID: " + queryData.taskId + " ignoring results");
                }
                obsolete = true;
                queryData.hostEntryInfo.setIgnored();
                return false;
            } else {
                return true;
            }
        }

        public String getJobId() {
            return queryData.jobId;
        }

        @Override
        public void cancel(String message) {
            canceled = true;
            //Calls done which closes connection to consumer / entity what asked us a query
            consumer.sourceError(new DataChannelError(message));
            try {
                //Close worker connections
                if (dataChannelReader != null) {
                    dataChannelReader.close();
                }
            } catch (Exception e) {
                log.warn("Exception closing dataChannelReader: " + e.getMessage());
            }
        }

        public void close() {
            try {
                if (dataChannelReader != null) {
                    dataChannelReader.close();
                }
            } catch (Exception e) {
                log.warn("Exception closing dataChannelReader: " + e.getMessage());
            }
        }

        public Integer getTaskId() {
            return queryData.taskId;
        }

        public String getKey() {
            return getJobId() + "/" + getTaskId();
        }

        public String toString() {
            return queryData.jobId + "/" + queryData.taskId;
        }

        public QuerySource createCloneWithReplacementFileReference(FileReference fileReference) {
            QueryData cloneQueryData = new QueryData(this.queryData.channelMaster, fileReference, this.queryData.queryOptions, this.getJobId(), this.getTaskId());
            return new QuerySource(cloneQueryData, consumer, query);
        }
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


    private static class QueryWatcher {

        private final Query query;
        private final AggregateHandle handle;
        private final MeshSourceAggregator meshSourceAggregator;

        private QueryWatcher(Query query, AggregateHandle handle, MeshSourceAggregator meshSourceAggregator) {
            this.query = query;
            this.handle = handle;
            this.meshSourceAggregator = meshSourceAggregator;
        }
    }

    private static class StragglerCheckThread extends Thread {

        private ConcurrentHashMap<String, QueryWatcher> activeQueryMap = new ConcurrentHashMap<String, QueryWatcher>();

        public StragglerCheckThread() {
            setDaemon(true);
            setName("StragglerCheckThread");
            start();
        }

        public void addQuery(Query query, AggregateHandle handle, MeshSourceAggregator meshSourceAggregator) {
            activeQueryMap.put(query.uuid(), new QueryWatcher(query, handle, meshSourceAggregator));

        }

        public void removeQuery(Query query) {
            activeQueryMap.remove(query.uuid());
        }

        @Override
        public void run() {
            while (!exiting.get()) {
                for (QueryWatcher queryWatcher : activeQueryMap.values()) {
                    boolean isStraggler;
                    if (useStdDevStragglers) {
                        isStraggler = checkForStragglers(queryWatcher);
                    } else {
                        isStraggler = checkForStragglersLegacy(queryWatcher);
                    }

                    if (isStraggler) {
                        activeQueryMap.remove(queryWatcher.query.uuid());
                    }
                }

                try {
                    Thread.sleep(stragglerCheckPeriod);
                } catch (InterruptedException e) {
                    log.warn("Straggler checker was interrupted; exiting");
                }
            }
        }

        private boolean checkForStragglers(QueryWatcher queryWatcher) {
            AggregateHandle handle = queryWatcher.handle;
            Query query = queryWatcher.query;

            if (handle.done.get()) {
                return true;
            }

            int totalTasks = handle._totalTasks;
            int numRemaining = totalTasks - handle.completed.size();
            int tasksDoneCutoff = Math.max(1, (int) Math.ceil(stragglerCheckHostPercentage * totalTasks));
            long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
            double timeCutoff = stragglerCheckMeanRuntimeFactor * handle.getMeanRuntime();

            if (numRemaining == 0) {
                if (log.isDebugEnabled() || query.isTraced()) {
                    Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
                }
                return true;
            } else if (numRemaining <= tasksDoneCutoff && elapsedTime > handle.getStdDevsAwayRuntime(MULTIPLE_STD_DEVS)) {
                if (log.isTraceEnabled()) {
                    log.trace("Running stragglers for query: " + query.uuid());
                    log.trace("numRemaining: " + numRemaining +
                             " taskDoneCutoff: " + tasksDoneCutoff +
                             " deltaTime: " + elapsedTime +
                             " " + MULTIPLE_STD_DEVS + " stdDevsAway: " + handle.getStdDevsAwayRuntime(MULTIPLE_STD_DEVS) +
                             " Mean runtime: " + handle.getMeanRuntime());
                }

                for (int node = 0; node < totalTasks; node++) {
                    if (!handle.isComplete(node) && !handle.isStarted(node)) {
                        synchronized (handle.sourcesByTaskID) {
                            Set<QueryData> queryDataSet = handle.sourcesByTaskID.get(node);
                            if (queryDataSet != null && queryDataSet.size() > 0) {
                                Iterator<QueryData> iterator = queryDataSet.iterator();
                                if (iterator.hasNext()) {
                                    QueryData queryData = iterator.next();
                                    totalStragglerCheckerRequests.inc();
                                    iterator.remove();
                                    String id = queryWatcher.meshSourceAggregator.requestQueryData(queryData, query);
                                    if (log.isDebugEnabled() || query.isTraced()) {
                                        log.warn("Straggler detected for " + query.uuid() + " node " + node + " sending duplicate query to host: " + queryData.hostEntryInfo.getHostName() + " sourceId: " + id);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return false;
        }

        private boolean checkForStragglersLegacy(QueryWatcher queryWatcher) {
            AggregateHandle handle = queryWatcher.handle;
            Query query = queryWatcher.query;
            if (handle.done.get()) {
                return true;
            }
            int totalTasks = handle._totalTasks;
            int numRemaining = totalTasks - handle.completed.size();
            int tasksDoneCutoff = Math.max(1, (int) Math.ceil(stragglerCheckHostPercentage * totalTasks));
            long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
            double timeCutoff = stragglerCheckMeanRuntimeFactor * handle.getMeanRuntime();
            if (numRemaining == 0) {
                if (log.isDebugEnabled() || query.isTraced()) {
                    Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
                }
                return true;
            } else if (numRemaining <= tasksDoneCutoff && (elapsedTime > timeCutoff)) {
                handle.handleStragglers();
            }
            return false;
        }
    }
}

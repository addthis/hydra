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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.bundle.io.DataChannelWriter;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryEngine;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.query.util.FramedDataChannelReader;
import com.addthis.meshy.LocalFileHandler;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileInput;
import com.addthis.meshy.VirtualFileReference;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.sleepycat.je.EnvironmentFailureException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where all the action happens for a mesh query worker. Its class name is passed as a system property to meshy as a local handler,
 * and is otherwise called from nowhere except to access some static variables. Despite the existence of a MeshQueryWorker class, this
 * is the most important class (followed closely by QueryEngine).
 * <p/>
 * Here we process queries, keep track of how many queries we are running, maintain a query engine cache, and queue up queries
 * when we are running our max. Currently, the biggest concern is the engine cache -- after the query cache in QueryTracker, it is
 * more or less the highest level cache that queries will run into. Each engine has its own BDB environment and page cache, and
 * attendent lower level caches (many), so they can consume quite a bit of memory. They are also very important for performance and
 * if they take a long time to open can block many queries from running.
 * <p/>
 * The basic flow is :
 * Query comes in as a QueryReference.getInput() call
 * A SearchRunner is created to get bundles
 * The SearchRunner is handed a bridge to push bundles to
 * Meshy is handed a reference to the bridge and takes bundles from it as bytes
 * <p/>
 * Other interesting query worker classes (used from SearchRunner): QueryEngine, QueryEngineCache
 * <p/>
 * Existing docs preserved here:
 * Date: 4/29/12
 * Time: 7:34 PM
 * <p/>
 * EXAMPLE IMPLEMENTATION
 * <p/>
 * server start:
 * java -Dmesh.local.handlers=com.addthis.hydra.query.MeshQuerySource -jar hydra-config/target/hydra-config-3.9.0-SNAPSHOT-exec.jar mesh server 5000 hydra-local/
 * <p/>
 * client query:
 * java -cp hydra-config/target/hydra-config-3.9.0-SNAPSHOT-exec.jar com.addthis.hydra.query.util.MeshQueryClient localhost 5000 'mini2n/f00f6eb5-805f-41b2-a407-575c618d6726/0/replica/data/query' '+:+hits,+nodes' ''
 * <p/>
 * at the moment query ops aren't working mysteriously
 * signaling needs improvement, etc
 */
public class MeshQuerySource implements LocalFileHandler {

    private static final Logger log = LoggerFactory.getLogger(MeshQuerySource.class);
    private static final String queryRoot = Parameter.value("mesh.query.root", "query");
    private static final int querySearchThreads = Parameter.intValue("meshQuerySource.searchThreads", 3);
    private static final int outputQueueSize = Parameter.intValue("meshQuerySource.outputQueueSize", 1000);
    private static final int outputBufferSize = Parameter.intValue("meshQuerySource.outputBufferSize", 64000);
    private static final int gateCounter = Parameter.intValue("meshQuerySource.gateCounter", 2); // max concurrent queries
    private static final int gateAcquireTimeLimit = Parameter.intValue("meshQuerySource.gateAcquireTimeLimit", 1000); // conservative
    private static final boolean busyResponseEnabled = Parameter.boolValue("meshQuerySource.busyResponseEnabled", false);
    //File that contains the next parent id to be assigned in the query tree, apparently.
    private static final String queryReferenceFileName = Parameter.value("meshQuerySource.queryReferenceFile", "nextID");
    private static final int slowQueryThreshold = Parameter.intValue("meshQuerySource.slowQueryThreshold", 5000);
    private static final int shutdownSleepPeriod = Parameter.intValue("meshQuerySource.shutdownSleepPeriod", 1000);
    private static final int environmentFailureCode = Parameter.intValue("meshQuerySource.envFailCode", 33);
    //Temp directory to use for sorting and caching
    private static final String tmpDirPath = Parameter.value("query.tmpdir", "query.tmpdir");
    private static final ExecutorService querySearchPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(querySearchThreads, querySearchThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("querySearch-%d").build()), 5, TimeUnit.SECONDS);
    // metrics
    private static final Meter forcedShutdownMeter = Metrics.newMeter(MeshQuerySource.class, "forcedShutdown", "shutdown", TimeUnit.MINUTES);
    //Query run times
    private static final Timer queryTimes = Metrics.newTimer(MeshQuerySource.class, "queryTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Time to get an engine from the engine cache -- hits should be tiny, misses should be quite different
    private static final Timer engineGetTimer = Metrics.newTimer(MeshQuerySource.class, "engineGetTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Time it takes for a query to be instantiated and given a gate.
    private static final Timer gateAcquireTimer = Metrics.newTimer(MeshQuerySource.class, "gateAcquireTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Queries we have recieved but not finished processing (includes those waiting to run aka our queue)
    private static final Counter queryCount = Metrics.newCounter(MeshQuerySource.class, "queryCount");
    private static final QueryEngineCache queryEngineCache = new QueryEngineCache();
    private final Semaphore queryGate = new Semaphore(gateCounter);

    public MeshQuerySource() {
        log.info("[MeshQuerySource] started.  base directory={}", queryRoot);
        log.info("Max concurrent queries (gateCounter):{}", gateCounter);

        // Initialize the tmp dir
        try {
            File tmpDir = new File(tmpDirPath).getCanonicalFile();
            Files.deleteDir(tmpDir);
            Files.initDirectory(tmpDir);
            log.info("Using temporary directory:{}", tmpDir.getPath());
        } catch (Exception e) {
            log.warn("Error while cleaning or obtaining canonical path for tmpDir: {}", tmpDirPath, e);
        }
    }

    @Override
    public boolean canHandleDirectory(File dir) {
        return new File(dir, "db.type").isFile() || new File(dir, "nextID").isFile();
    }

    @Override
    public Iterator<VirtualFileReference> listFiles(File dir, VirtualFileFilter filter) {
        ArrayList<VirtualFileReference> list = new ArrayList<>(2);
        list.add(new QueryReference(dir));
        return list.iterator();
    }

    /**
     * queryRoot is defined by system parameters, but is usually 'query' and thus the the 'query' part of
     * query is only used a flag/validation method.
     *
     * @param dir  - the data directory
     * @param name - the query root / query flag
     * @return QueryReference for the data directory
     */
    @Override
    public VirtualFileReference getFile(File dir, String name) {
        return name.equals(queryRoot) ? new QueryReference(dir) : null;
    }

    /**
     * virtualizes queries
     * <p/>
     * This class's getInput method is the main entry point for all queries to this mqworker. It is the first class in the three
     * step query process.
     * <p/>
     * By virtualizes queries, we mean that it performs queries and gets a response in a way that meshy can understand. As the
     * class is named 'MeshQuerySource', this makes sense. As the primary class and entry point for mq worker function, it might be
     * worth this explanation to prevent any confusion.
     */
    private final class QueryReference implements VirtualFileReference {

        final File dir;
        final String dirString;
        final File queryReferenceFile;

        QueryReference(final File dir) {
            try {
                this.dir = dir.getCanonicalFile();
                this.dirString = dir.toString();
                queryReferenceFile = new File(dir, MeshQuerySource.queryReferenceFileName);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public String toString() {
            return dirString;
        }

        @Override
        public String getName() {
            return queryRoot;
        }

        @Override
        public long getLastModified() {
            return queryReferenceFile.lastModified();
        }

        @Override
        public long getLength() {
            return queryReferenceFile.length();
        }

        @Override
        public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
            return null;
        }

        @Override
        public VirtualFileReference getFile(String name) {
            return null;
        }

        /**
         * Submits the query to the query search pool as a SearchRunner and creates the bridge that will
         * hand the query response data to meshy.
         *
         * @param options
         * @return the response bridge (DataChannelToInputStream)
         */
        @Override
        public VirtualFileInput getInput(Map<String, String> options) {
            try {
                final DataChannelToInputStream bridge = new DataChannelToInputStream();
                if (options == null) {
                    log.warn("Invalid request to getInput.  Options cannot be null");
                    return null;
                }
                querySearchPool.submit(new SearchRunner(options, dirString, bridge));
                return bridge;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * The class that performs the querying and feeds bundles into the bridge. The second class in the three step query process.
     * <p/>
     * Flow is : constructor -> run
     */
    private class SearchRunner implements Runnable {

        private final Map<String, String> options;
        private final String goldDirString;
        /**
         * A reference to {@link DataChannelToInputStream}. Meshy has a reference to this object using
         * the {@link DataChannelOutput} interface and uses it to call {@link DataChannelToInputStream#nextBytes(long)}.
         */
        private final DataChannelToInputStream bridge;
        boolean acquired = false;
        long gateAcquisitionDuration;
        private Query query;
        private QueryOpProcessor queryOpProcessor = null;
        private QueryEngine finalEng = null;

        public SearchRunner(final Map<String, String> options, final String dirString,
                final DataChannelToInputStream bridge) throws Exception {
            // Get the canonical path and store it in the canonicalDirString. Typically, we will receive a gold path
            // here, which is a symlink.
            this.goldDirString = dirString;
            this.bridge = bridge;
            this.options = options;
        }

        @Override
        public void run() {
            queryCount.inc();
            try {
                gating();
                setup();
                engine();
                search();
                //success
            } catch (DataChannelError ex) //TODO -- Handle cancels better (less verbosely)
            {
                log.warn("DataChannelError while running search, query was likely canceled on the server side," +
                         " {} or the query execution got a Thread.interrupt call", ex.getMessage());
                logError(ex);
                if (queryOpProcessor != null) {
                    queryOpProcessor.sourceError(ex);
                    queryOpProcessor.sendComplete();
                }
            } catch (EnvironmentFailureException ex) //BDB specific exception
            {
                log.warn("******** Received an environment failure exception{}", ex.getMessage());
                logError(ex);
                new ShutdownRunner(shutdownSleepPeriod, environmentFailureCode).start();
                if (queryOpProcessor != null) {
                    queryOpProcessor.sourceError(new DataChannelError(ex));
                    queryOpProcessor.sendComplete();
                }
            } catch (Exception ex) {
                log.warn("Generic Exception while running search. {}", ex.getMessage());
                logError(ex);
                if (ex.getMessage().contains("EnvironmentFailure")) {
                    log.warn("Doing a system exit to restart the mesh query source (from generic exception)");
                    new ShutdownRunner(shutdownSleepPeriod, environmentFailureCode).start();
                }
                if (queryOpProcessor != null) {
                    queryOpProcessor.sourceError(new DataChannelError(ex));
                    queryOpProcessor.sendComplete();
                }
            }

            // Cleanup -- decrease query count, release our engine (if we had one), release our gate.
            //
            // Note that releasing our engine only decreases its open lease count.
            finally {
                queryCount.dec();
                if (finalEng != null) {
                    log.debug("Releasing engine: {}", finalEng);
                    finalEng.release();
                }

                if (acquired) {
                    log.debug("releasing query gate for: {}", goldDirString);
                    queryGate.release();
                }
            }
        }

        private void logError(Throwable ex) {
            log.warn("Canonical directory: {}", goldDirString);
            log.warn("Engine: {}", finalEng);
            log.warn("Query options: uuid={}", options, ex);
            // See if we can send the error to mqmaster as well
            if (queryOpProcessor == null) {
                queryOpProcessor = Query.createProcessor(bridge, bridge.queryStatusObserver);
            }
        }

        /**
         * Part 4 - SEARCH
         * Run the search -- most of this logic is in QueryEngine.search(). We only take care of logging times and
         * passing the sendComplete message along.
         */
        private void search() {
            final long searchStartTime = System.currentTimeMillis();
            finalEng.search(query, queryOpProcessor, bridge.getQueryStatusObserver());
            queryOpProcessor.sendComplete();
            final long searchDuration = System.currentTimeMillis() - searchStartTime;
            if (log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("[QueryReference] search complete " + query.uuid() + " in " + searchDuration + "ms directory: " +
                                goldDirString + " slow=" + (searchDuration > slowQueryThreshold) + " rowsIn: " + queryOpProcessor.getInputRows());
            }
            queryTimes.update(searchDuration, TimeUnit.MILLISECONDS);
        }

        /**
         * Part 3 - ENGINE CACHE
         * Get a QueryEngine for our query -- check the cache for a suitable candidate, otherwise make one.
         * Most of this logic is handled by the QueryEngineCache.get() function.
         */
        private void engine() throws Exception {
            final long engineGetStartTime = System.currentTimeMillis();
            // Use the canonical path stored in the canonicalDirString to create a QueryEngine. By that way
            // if the alias changes new queries will use the latest available
            // database and the old engines will be automatically closed after their TTL expires.
            finalEng = queryEngineCache.getAndLease(goldDirString);
            final long engineGetDuration = System.currentTimeMillis() - engineGetStartTime;
            engineGetTimer.update(engineGetDuration, TimeUnit.MILLISECONDS);

            if (finalEng == null) //Cache returned null -- this doesn't mean cache miss. It means something went fairly wrong
            {
                log.warn("[QueryReference] Unable to retrieve queryEngine for query: {}, key: {} after waiting: {}ms",
                        query.uuid(), goldDirString, engineGetDuration);
                throw new DataChannelError("Unable to retrieve queryEngine for query: " + query.uuid() +
                                           ", key: " + goldDirString + " after waiting: " + engineGetDuration + "ms");
            } //else we got an engine so we're good -- maybe this logic should be in the cache get

            if (engineGetDuration > slowQueryThreshold || log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("[QueryReference] Retrieved queryEngine for query: " + query.uuid() + ", key:" +
                                goldDirString + " after waiting: " + engineGetDuration + "ms.  slow=" +
                                (engineGetDuration > slowQueryThreshold));
            }
        }

        /**
         * Part 2 - SETUP
         * Initialize query run -- parse options, create Query object
         */
        private void setup() throws Exception {
            query = CodecJSON.decodeString(new Query(), options.get("query"));
            // Log some information about how long gate acquisition took
            if (query.isTraced() || gateAcquisitionDuration > slowQueryThreshold) {
                Query.emitTrace("[MeshQuerySource] query:" + query.uuid() + " gate acquisitionDuration=" + gateAcquisitionDuration + "ms, slow=" + (gateAcquisitionDuration > slowQueryThreshold));
            }
            // Parse the query and return a reference to the last QueryOpProcessor.
            queryOpProcessor = query.getProcessor(bridge, bridge.queryStatusObserver);
        }

        /**
         * Part 1 - GATING
         * Try to assign this new query to one our query slots. If busy signal is enabled, periodically let mqmaster know
         * we are alive but still too busy to run this query.
         * <p/>
         * Note that fairness is not guaranteed (and can't be when the busy signal is enabled) -- therefore when at capacity,
         * a given query may theoretically block indefinitely. Fortunately we are not usually at capacity, but heads up!
         */
        private void gating() throws InterruptedException {
            final long queryStartTime = System.currentTimeMillis();

            final String flag = options.get("flag");
            if (flag != null) {
                if (flag.equals("die")) {
                    System.exit(1);
                } else if (flag.equals("DIE")) {
                    Runtime.getRuntime().halt(1);
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[MeshQuerySource] getting query gate for job: " + options.get("jobid") +
                          " query:" + options.get("uuid") + " queue length: " + queryGate.getQueueLength() +
                          " remaining leases:" + queryGate.availablePermits() + " max:" + gateCounter +
                          " queryCount:" + queryCount.count());
            }

            while (!acquired) {
                if (busyResponseEnabled) {
                    if (queryGate.tryAcquire(gateAcquireTimeLimit, TimeUnit.MILLISECONDS)) {
                        acquired = true;
                    } else {
                        bridge.sourceBusy();
                    }
                } else //doesn't really need to be in this while loop
                {
                    queryGate.acquire();
                    acquired = true;
                }
            }

            gateAcquisitionDuration = System.currentTimeMillis() - queryStartTime;
            gateAcquireTimer.update(gateAcquisitionDuration, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This class is the last point the bundles reach before getting converted into bytes and read by meshy to be
     * sent over the network to the client (MQMaster). The last class in the three step query process.
     */
    private static class DataChannelToInputStream implements DataChannelOutput, VirtualFileInput, BundleFormatted {

        private final KVBundleFormat format = new KVBundleFormat();
        private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(outputQueueSize);
        private final DataChannelWriter writer;
        private final ByteArrayOutputStream out;
        /**
         * A wrapper for a boolean flag that gets set if close is called. This observer object will be passed all
         * the way down to {@link QueryEngine#tableSearch(java.util.LinkedList, com.addthis.hydra.data.tree.DataTreeNode, com.addthis.hydra.data.query.FieldValueList, com.addthis.hydra.data.query.QueryElement[], int, com.addthis.bundle.channel.DataChannelOutput, int, com.addthis.hydra.data.query.QueryStatusObserver)}.
         */
        public QueryStatusObserver queryStatusObserver = new QueryStatusObserver();
        private int rows = 0;
        /**
         * A boolean flag that gets set to true once all the data have been sent to the stream, and not necessarily
         * pulled or read from the stream.
         */
        private volatile boolean eof;
        /**
         * Stores true if close() has been called.
         */
        private volatile boolean closed = false;

        /**
         * A non-public constructor. This class can only be instantiated from it outer class MeshQueryMaster. The objects
         * can be accessed elsewhere using the interfaces.
         *
         * @throws Exception
         */
        DataChannelToInputStream() throws Exception {
            out = new ByteArrayOutputStream();
            writer = new DataChannelWriter(out);
        }

        /**
         * This function is called by meshy and it convers the data pushed to the out ByteArrayOutputStream into
         * byte[], which gets read by meshy and sent over the channel to the source.
         *
         * @param wait the amount of milliseconds to wail polling for data.
         * @return a byte array if data exists in the OutputStream otherwise returns null if the timeout expires.
         */
        @Override
        public byte[] nextBytes(long wait) {
            if (closed || isEOF()) {
                log.debug("EOF reached. rows={}", rows);
                return null;
            }
            byte[] data;
            try {
                data = queue.poll(wait, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                log.warn("Interrupted while waiting for data");
                eof = true;
                return null;
            }
            if (data == null && out.size() > 0) {
                emitChunks();
                data = queue.poll();
            }
            return data;
        }

        private void emitChunks() {
            synchronized (out) {
                byte[] bytes = out.toByteArray();
                out.reset();
                try {
                    int length = bytes.length;
                    if (length > 0) {
                        byte[] chunk = new byte[length];
                        System.arraycopy(bytes, 0, chunk, 0, length);
                        for (int i = 0; i < 100; i++) //Try adding to queue 100 times
                        {
                            if (queue.offer(chunk, 1000L, TimeUnit.MILLISECONDS)) {
                                if (i > 10) {
                                    log.warn("Managed to add to output queue on the {} attempt", i);
                                }
                                return;
                            }
                            checkClosed();
                        }
                        throw new DataChannelError("Output queue oversized for 100 attempts");
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted while putting bytes onto output buffer");
                    throw new DataChannelError("interrupted", e);
                }
            }
        }

        /**
         * Checks whether or not {@link #closed} was set to true. If {@link #closed} has been set to true then
         * it means there has either been a failure up stream or the query has been canceled.
         * In either case we want to stop the running of this stream and at that point this function will throw
         * a DataChannelError exception.
         */
        private void checkClosed() {
            if (closed) {
                throw new DataChannelError("Query Closed by upstream action");
            }
        }

        /**
         * Returns true if the eof flag is set and there is no data queued in the stream to be sent.
         *
         * @return true if EOF otherwise false.
         */
        @Override
        public boolean isEOF() {
            synchronized (out) {
                return eof && queue.isEmpty() && out.size() == 0;
            }
        }

        /**
         * Tells this channel to close. It sets the closed flag and the queryStatusObserver to true.
         */
        @Override
        public void close() {
            closed = true;
            queryStatusObserver.queryCancelled = true;
        }

        /**
         * Takes in a list of bundles, loops on them and calls send for each one.
         *
         * @param bundles
         */
        @Override
        public void send(List<Bundle> bundles) {
            checkClosed(); // Just in case the list was empty, we check if the channel is closed here
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }

        /**
         * Takes in a bundle and writes it on the writer (mapped to out), which encodes the bundle to bytes.
         *
         * @param bundle
         * @throws DataChannelError
         */
        @Override
        public void send(Bundle bundle) throws DataChannelError {
            checkClosed();
            try {
                synchronized (out) {
                    out.write(FramedDataChannelReader.FRAME_MORE);
                    writer.write(bundle);
                    if (out.size() > outputBufferSize) {
                        emitChunks();
                    }
                    rows++;
                }
            } catch (IOException ex) {
                throw new DataChannelError(ex);
            }
        }

        /**
         * Is called when all the data has been sent. It sets the eof flag and writes an EOF marker on the output
         * stream.
         */
        @Override
        public void sendComplete() {
            checkClosed();
            synchronized (out) {
                out.write(FramedDataChannelReader.FRAME_EOF);
                emitChunks();
                eof = true;
            }
        }

        /**
         * This function gets called when an error is encountered from the source.
         *
         * @param er error encountered from the source
         */
        @Override
        public void sourceError(DataChannelError er) {
            checkClosed();
            try {
                // if we know writer is closed, don't try to write to it.
                if (!writer.isClosed()) {
                    synchronized (out) {
                        out.write(FramedDataChannelReader.FRAME_ERROR);
                        Bytes.writeString(er.getClass().getCanonicalName(), out);
                        Bytes.writeString(er.getMessage(), out);
                        emitChunks();
                        eof = true;
                    }
                }
            } catch (Exception ex) {
                throw new DataChannelError(ex);
            }
        }

        /**
         * When called, this function will write a {@link FramedDataChannelReader#FRAME_BUSY} marker to the output channel.
         */
        public void sourceBusy() {
            checkClosed();
            try {
                if (!writer.isClosed()) {
                    synchronized (out) {
                        out.write(FramedDataChannelReader.FRAME_BUSY);
                    }
                }
            } catch (Exception ex) {
                throw new DataChannelError(ex);
            }
        }

        @Override
        public Bundle createBundle() {
            return new KVBundle(format);
        }

        @Override
        public BundleFormat getFormat() {
            return format;
        }

        /**
         * @return a reference to the {@link #queryStatusObserver}.
         */
        public QueryStatusObserver getQueryStatusObserver() {
            return queryStatusObserver;
        }

    }

    /**
     * A thread that shuts down the JVM. Usually called in response to some kind of unrecoverable error.
     * <p/>
     * The sleep call is to attempt to give the query worker some time to report an error to the master.
     */
    private static class ShutdownRunner extends Thread {

        private final int sleep;
        private final int shutdownCode;

        private ShutdownRunner(int sleep, int shutdownCode) {
            this.sleep = sleep;
            this.shutdownCode = shutdownCode;
        }

        @Override
        public void run() {
            try {
                forcedShutdownMeter.mark();
                sleep(sleep);
            } catch (InterruptedException ignored) {
            }
            System.exit(shutdownCode);
        }
    }
}

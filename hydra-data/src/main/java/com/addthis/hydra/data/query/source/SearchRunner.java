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

package com.addthis.hydra.data.query.source;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.engine.QueryEngine;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/**
 * The class that performs the querying and feeds bundles into the bridge. The second class in the three step query process.
 * <p/>
 * Flow is : constructor -> run
 */
class SearchRunner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SearchRunner.class);

    static final int querySearchThreads = Parameter.intValue("meshQuerySource.searchThreads", 3);
    static final ExecutorService querySearchPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(querySearchThreads, querySearchThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("querySearch-%d").build()), 5, TimeUnit.SECONDS);


    private final Map<String, String> options;
    private final String goldDirString;
    /**
     * A reference to {@link com.addthis.hydra.data.query.source.DataChannelToInputStream}. Meshy has a reference to this object using
     * the {@link com.addthis.bundle.channel.DataChannelOutput} interface and uses it to call {@link com.addthis.hydra.data.query.source.DataChannelToInputStream#nextBytes(long)}.
     */
    private final DataChannelToInputStream bridge;
    private final long creationTime;
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
        this.creationTime = System.currentTimeMillis();
    }

    @Override
    public void run() {
        MeshQuerySource.queryCount.inc();
        try {
            setup();
            finalEng = getEngine();
            search();
            //success
        } catch (CancellationException ignored) {
            log.info("query was cancelled remotely; stopping processing early");
        } catch (DataChannelError ex) {
            log.warn("DataChannelError while running search, query was likely canceled on the " +
                     "server side, or the query execution got a Thread.interrupt call", ex);
            reportError(ex);
        } catch (Exception ex) {
            log.warn("Generic Exception while running search.", ex);
            reportError(ex);
        }

        // Cleanup -- decrease query count, release our engine (if we had one)
        //
        // Note that releasing our engine only decreases its open lease count.
        finally {
            MeshQuerySource.queryCount.dec();
            if (finalEng != null) {
                log.debug("Releasing engine: {}", finalEng);
                finalEng.release();
            }
        }
    }

    protected void reportError(Throwable ex) {
        log.warn("Canonical directory: {}", goldDirString);
        log.warn("Engine: {}", finalEng);
        log.warn("Query options: uuid={}", options, ex);

        if (!(ex instanceof DataChannelError)) {
            ex = new DataChannelError(ex);
        }
        DataChannelError error = (DataChannelError) ex;
        // See if we can send the error to mqmaster as well
        if (queryOpProcessor != null) {
            queryOpProcessor.sourceError(error);
            queryOpProcessor.sendComplete();
        } else {
            bridge.sourceError(error);
        }
    }

    /**
     * Part 1 - SETUP
     * Initialize query run -- parse options, create Query object
     */
    protected void setup() throws Exception {
        long startTime = System.currentTimeMillis();
        MeshQuerySource.queueTimes.update(creationTime - startTime, TimeUnit.MILLISECONDS);
        query = CodecJSON.decodeString(new Query(), options.get("query"));
        // set as soon as possible (and especially before creating op processor)
        query.queryPromise = bridge.queryPromise;
        // Parse the query and return a reference to the last QueryOpProcessor.
        ChannelProgressivePromise opPromise =
                new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE);
        queryOpProcessor = query.newProcessor(bridge, opPromise);
    }

    /**
     * Part 2 - ENGINE CACHE
     * Get a QueryEngine for our query -- check the cache for a suitable candidate, otherwise make one.
     * Most of this logic is handled by the QueryEngineCache.get() function.
     */
    protected QueryEngine getEngine() throws Exception {
        final long engineGetStartTime = System.currentTimeMillis();
        // Use the canonical path stored in the canonicalDirString to create a QueryEngine. By that way
        // if the alias changes new queries will use the latest available
        // database and the old engines will be automatically closed after their TTL expires.
        QueryEngine engine = MeshQuerySource.queryEngineCache.getAndLease(goldDirString);
        final long engineGetDuration = System.currentTimeMillis() - engineGetStartTime;
        MeshQuerySource.engineGetTimer.update(engineGetDuration, TimeUnit.MILLISECONDS);

        if (engine == null) //Cache returned null -- this doesn't mean cache miss. It means something went fairly wrong
        {
            log.warn("[QueryReference] Unable to retrieve queryEngine for query: {}, key: {} after waiting: {}ms",
                    query.uuid(), goldDirString, engineGetDuration);
            throw new DataChannelError("Unable to retrieve queryEngine for query: " + query.uuid() +
                                       ", key: " + goldDirString + " after waiting: " + engineGetDuration + "ms");
        } //else we got an engine so we're good -- maybe this logic should be in the cache get

        if (engineGetDuration > MeshQuerySource.slowQueryThreshold || log.isDebugEnabled() || query.isTraced()) {

            Query.traceLog.info(
                    "[QueryReference] Retrieved queryEngine for query: " + query.uuid() + ", key:" +
                                goldDirString + " after waiting: " + engineGetDuration + "ms.  slow=" +
                                (engineGetDuration > MeshQuerySource.slowQueryThreshold));
        }
        return engine;
    }

    /**
     * Part 3 - SEARCH
     * Run the search -- most of this logic is in QueryEngine.search(). We only take care of logging times and
     * passing the sendComplete message along.
     */
    protected void search() {
        final long searchStartTime = System.currentTimeMillis();
        finalEng.search(query, queryOpProcessor, bridge.getQueryPromise());
        queryOpProcessor.sendComplete();
        final long searchDuration = System.currentTimeMillis() - searchStartTime;
        if (log.isDebugEnabled() || query.isTraced()) {
            Query.traceLog.info(
                    "[QueryReference] search complete " + query.uuid() + " in " + searchDuration + "ms directory: " +
                                goldDirString + " slow=" + (searchDuration > MeshQuerySource.slowQueryThreshold) + " rowsIn: " + queryOpProcessor.getInputRows());
        }
        MeshQuerySource.queryTimes.update(searchDuration, TimeUnit.MILLISECONDS);
    }
}

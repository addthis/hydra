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

import java.io.File;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.data.query.QueryEngineCache;
import com.addthis.meshy.LocalFileHandler;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileReference;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
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
 * java -Dmesh.local.handlers=com.addthis.hydra.data.query.source.MeshQuerySource -jar hydra-config/target/hydra-config-3.9.0-SNAPSHOT-exec.jar mesh server 5000 hydra-local/
 * <p/>
 * client query:
 * java -cp hydra-config/target/hydra-config-3.9.0-SNAPSHOT-exec.jar com.addthis.hydra.query.util.MeshQueryClient localhost 5000 'mini2n/f00f6eb5-805f-41b2-a407-575c618d6726/0/replica/data/query' '+:+hits,+nodes' ''
 * <p/>
 * at the moment query ops aren't working mysteriously
 * signaling needs improvement, etc
 */
public class MeshQuerySource implements LocalFileHandler {

    static final Logger log = LoggerFactory.getLogger(MeshQuerySource.class);

    static final int slowQueryThreshold = Parameter.intValue("meshQuerySource.slowQueryThreshold", 5000);

    //Query run times
    static final Timer queryTimes = Metrics.newTimer(MeshQuerySource.class, "queryTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Time to get an engine from the engine cache -- hits should be tiny, misses should be quite different
    static final Timer engineGetTimer = Metrics.newTimer(MeshQuerySource.class, "engineGetTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Time it takes for a query to be instantiated and given a gate.
    static final Timer queueTimes = Metrics.newTimer(MeshQuerySource.class, "queueTimes", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    //Queries we have recieved but not finished processing (includes those waiting to run aka our queue)
    static final Counter queryCount = Metrics.newCounter(MeshQuerySource.class, "queryCount");

    static final QueryEngineCache queryEngineCache = new QueryEngineCache();

    //Temp directory to use for sorting and caching
    private static final String tmpDirPath = Parameter.value("query.tmpdir", "query.tmpdir");

    public MeshQuerySource() {
        log.info("[MeshQuerySource] started.  base directory={}", QueryReference.queryRoot);
        log.info("Max concurrent queries (thread count):{}", SearchRunner.querySearchThreads);

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
        return name.equals(QueryReference.queryRoot) ? new QueryReference(dir) : null;
    }
}

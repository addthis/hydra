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
package com.addthis.hydra.task.output.tree;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.Bench;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.query.source.LiveMeshyServer;
import com.addthis.hydra.data.query.source.LiveQueryReference;
import com.addthis.hydra.data.tree.ConcurrentTree;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.TreeCommonParameters;
import com.addthis.hydra.data.util.TimeField;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.task.output.DataOutputTypeList;
import com.addthis.hydra.task.output.tree.TreeMapperStats.Snapshot;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.meshy.MeshyServer;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output <span class="hydra-summary">transforms bundle streams into trees for statistical analysis and data queries</span>
 * <p/>
 * <p>A tree is defined by one or more paths. A path is a reusable set of connected tree nodes. One of these
 * paths is designated as the root path, from which the rest of the tree is constructed.</p>
 * <p/>
 * <p>A tree may optionally specify a feature set. The feature set is a set of character strings.
 * Path elements that do not match against the feature set will not be included in the tree.
 * If a path element specifies the {@link PathElement#feature feature} parameter then the path
 * element is excluded if it specifies a feature that is not in the feature set of the tree.
 * If a path element specifies the {@link PathElement#featureOff featureOff} parameter then the path
 * element is excluded if it specifies a feature that is in the feature set of the tree.
 * <p/>
 * <p>Example:</p>
 * <pre>output : {
 *   type : "tree",
 *   live : true,
 *   timeField : {
 *     field : "TIME",
 *     format : "native",
 *   },
 *   root : { path : "ROOT" },
 * <p/>
 *   paths : {
 *     "ROOT" : [
 *       {type : "const", value : "date"},
 *       {type : "value", key : "DATE_YMD"},
 *       {type : "value", key : "DATE_HH"},
 *     ],
 *   },
 * },</pre>
 *
 * @user-reference
 * @hydra-name tree
 */
public final class TreeMapper extends DataOutputTypeList implements Codable {

    private static final Logger log = LoggerFactory.getLogger(TreeMapper.class);
    private static final DecimalFormat percent = new DecimalFormat("00.0%");
    private static final SimpleDateFormat date = new SimpleDateFormat("yyMMdd-HHmmss");

    private enum BENCH {
        TIME, UNITS, RULES, STREAM, LOCAL
    }

    private static enum ValidateMode {
        ALL, POST, NONE
    }

    /**
     * Default is either "mapper.printinterval" configuration value or 1000.
     */
    @FieldConfig(codable = true)
    private long printinterval = Parameter.longValue("mapper.printinterval", 1000L);

    /**
     * Definition of the tree structure.
     * Consists of a mapping from a name to one or more path elements.
     * One of these path elements will serve as the root of the tree.
     */
    @FieldConfig(codable = true)
    private HashMap<String, PathElement[]> paths;

    /**
     * Optional path that is processed once
     * at the beginning of execution.
     * The input to this path is an empty bundle.
     */
    @FieldConfig(codable = true)
    private TreeMapperPathReference pre;

    /**
     * Path that will serve as the root of the output tree.
     */
    @FieldConfig(codable = true)
    private TreeMapperPathReference root;

    /**
     * Optional path that is processed once
     * at the end of execution.
     * The input to this path is an empty bundle.
     */
    @FieldConfig(codable = true)
    private TreeMapperPathReference post;

    /**
     * Optional specify whether to perform
     * validation on the tree pages stored
     * in persistent storage. This is a slow operation.
     * "NONE" never validates.
     * "POST" validates only if "post" is fired.
     * "ALL" always validates. Default is "NONE".
     * If the tree pages are not valid then the task will error.
     * In the event of invalid tree pages the correct action
     * is to revert the task.
     */
    @FieldConfig(codable = true)
    private ValidateMode validateTree = ValidateMode.NONE;

    /**
     * If tree validation has been validated (see {@link #validateTree validateTree}
     * then this parameter determines whether
     * repairs will be made when an error is detected.
     * Default is false.
     */
    @FieldConfig(codable = true)
    private boolean repairTree = false;

    /**
     * Optional sample rate for applying
     * the {@link #post post} paths. If greater
     * than one than apply once every N runs.
     * Default is one.
     */
    @FieldConfig(codable = true)
    private int postRate = 1;

    /**
     * One or more queries that are executed
     * after the tree has been constructed.
     */
    @FieldConfig(codable = true)
    private PathOutput[] outputs;

    @FieldConfig(codable = true)
    private boolean live;

    @FieldConfig(codable = true)
    private String liveHost;

    @FieldConfig(codable = true)
    private int livePort;

    @FieldConfig(codable = true)
    private Integer nodeCache;

    @FieldConfig(codable = true)
    private Integer trashInterval;

    @FieldConfig(codable = true)
    private Integer trashTimeLimit;

    @FieldConfig(codable = true)
    private TimeField timeField;

    @FieldConfig(codable = true)
    private boolean stats = true;

    /**
     * Set of strings that enumerate the features to process.
     */
    @FieldConfig(codable = true)
    private HashSet<String> features;

    @FieldConfig(codable = true)
    private StoreConfig storage;

    @FieldConfig(codable = true)
    private int maxErrors = 0;

    @FieldConfig private boolean profiling = false;

    private final ConcurrentMap<String, BundleField> fields    = new ConcurrentHashMap<>();
    private final IndexHash<PathElement[]>           pathIndex = new IndexHash();

    private DataTree tree;
    private Bench    bench;
    private long     startTime;

    private MeshyServer     liveQueryServer;
    private TreeMapperStats mapstats;
    private TaskRunConfig   config;

    private final AtomicLong    lastHeaderTime  = new AtomicLong(JitterClock.globalTime());
    private final AtomicLong    benchCalls      = new AtomicLong(0);
    private final AtomicLong    streamWaitime   = new AtomicLong(0);
    private final AtomicLong    streamReadCount = new AtomicLong(0);
    private final AtomicLong    streamReadTotal = new AtomicLong(0);
    private final AtomicLong    mapWriteTime    = new AtomicLong(0);
    private final AtomicLong    processed       = new AtomicLong(0);
    private final AtomicLong    processNodes    = new AtomicLong(0);
    private       int           bundleErrors    = 0;
    private final AtomicLong    lastBundleTime  = new AtomicLong(0);

    private static class IndexHash<V> {

        private HashMap<String, Integer> map  = new HashMap<>();
        private ArrayList<V>             list = new ArrayList<>(64);

        public int add(String key, V value) {
            int pos = list.size();
            map.put(key, pos);
            list.add(value);
            return pos;
        }

        public V getValueByKey(String key) {
            return getValueByIndex(map.get(key));
        }

        public V getValueByIndex(Integer pos) {
            return list.get(pos);
        }

        public Integer getIndex(String key) {
            return map.get(key);
        }
    }

    public static final class StoreConfig implements Codable {

        @FieldConfig(codable = true)
        Integer memSample;
        @FieldConfig(codable = true)
        Integer maxCacheSize;
        @FieldConfig(codable = true)
        Integer maxPageSize;
        @FieldConfig(codable = true)
        Long    maxCacheMem;
        @FieldConfig(codable = true)
        Integer maxPageMem;

        public void setStaticFieldsFromMembers() {
            if (maxCacheSize != null)
                TreeCommonParameters.setDefaultMaxCacheSize(maxCacheSize);
            if (maxCacheMem != null)
                TreeCommonParameters.setDefaultMaxCacheMem(maxCacheMem);
            if (maxPageSize != null)
                TreeCommonParameters.setDefaultMaxPageSize(maxCacheSize);
            if (maxPageMem != null)
                TreeCommonParameters.setDefaultMaxPageMem(maxPageMem);
            if (memSample != null)
                TreeCommonParameters.setDefaultMemSample(memSample);
        }
    }

    private void resolve() throws Exception {
        fields.clear();
        if (features != null) {
            PathElement.featureSet.addAll(features);
        }
        // index paths and intern path element keys
        if (paths != null) {
            for (Map.Entry<String, PathElement[]> me : paths.entrySet()) {
                PathElement[] pe = me.getValue();
                for (PathElement p : pe) {
                    p.resolve(this);
                }
                pathIndex.add(me.getKey(), pe);
            }
        }
        if (pre != null) {
            pre.resolve(this);
        }
        if (root == null && paths != null && paths.size() > 0) {
            root = new TreeMapperPathReference(paths.keySet().iterator().next());
        }
        if (root != null) {
            root.resolve(this);
        }
        if (post != null) {
            post.resolve(this);
        }
        if (outputs != null) {
            for (PathOutput out : outputs) {
                out.resolve(this);
            }
        }
    }

    public PathElement[] getPath(String path) {
        return paths.get(path);
    }

    public Integer getPathIndex(String path) {
        return pathIndex.getIndex(path);
    }

    @Override
    public void open(TaskRunConfig config) {
        this.config = config;
        try {
            mapstats = new TreeMapperStats(log);
            resolve();

            if (nodeCache != null) TreeCommonParameters.setDefaultCleanQueueSize(nodeCache);
            if (trashInterval != null) TreeCommonParameters.setDefaultTrashInterval(trashInterval);
            if (trashTimeLimit != null) TreeCommonParameters.setDefaultTrashTimeLimit(trashTimeLimit);
            if (storage != null) storage.setStaticFieldsFromMembers();

            log.info("[init] live={}, target={} job={}", live, root, this.config.jobId);

            Path treePath = Paths.get(config.dir, "data");
            tree = new ConcurrentTree(Files.initDirectory(treePath.toFile()));
            bench = new Bench(EnumSet.allOf(BENCH.class), 1000);

            if (this.config.jobId != null && live && livePort > -1) {
                QueryEngine liveQueryEngine = new QueryEngine(tree);
                connectToMesh(treePath.toFile(), config.jobId, liveQueryEngine);
            }

            startTime = System.currentTimeMillis();

            if (pre != null) {
                log.warn("pre-chain: " + pre);
                processBundle(new KVBundle(), pre);
            }
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private void connectToMesh(File root, String jobId, QueryEngine engine) throws IOException {
        LiveQueryReference queryReference = new LiveQueryReference(root, jobId, engine);
        liveQueryServer = new LiveMeshyServer(livePort, queryReference);
    }

    public BundleField bindField(String key) {
        return getFormat().getField(key);
    }

    // ------------------------- PROCESSING ENGINE -------------------------

    public boolean isProfiling() {
        return profiling;
    }

    public void updateProfile(PathElement pathElement, long duration) {
        pathElement.updateProfile(duration);
    }

    public TreeMapperPathReference createBundleTarget(String rule) {
        TreeMapperPathReference pt = new TreeMapperPathReference(rule);
        pt.resolve(this);
        return pt;
    }

    /**
     * Processor interface take a packet and target and either hand it to local
     * delivery directly, or if processing queue is enabled, hand it to the
     * queue to be re-delivered. when the packet is re-delivered through the
     * queue, it goes to the processPacket method of the JobQueueItem, not
     * Hydra's processPacket method (this one). read deliverPacket() for more
     * information. process queues are a performance gain on nodes with >2
     * processors. when JobQueueItems are first created, they have a queue with
     * only one entry. if the execution of that rule calls other rules, they are
     * added to this job's queue rather than the process queue, which is bounded
     * and could lock up through recursive addition. this method iterates over
     * the JobQueueItems queue until it's emptied. for each entry, the packet
     * and target are examined and either delivered locally or sent to the
     * router for delivery to another hydra node.
     */
    public void processBundle(Bundle bundle, TreeMapperPathReference target) {
        try {
            Integer unit = target.getTargetUnit();
            if (unit == null) {
                log.warn("[deliver] target missing unit: " + target);
                return;
            }
            long bundleTime;
            try {
                bundleTime = getBundleTime(bundle);
            } catch (NumberFormatException nfe) {
                log.warn("error reading TimeField, : " + timeField.getField() + "\nbundle: " + bundle.toString());
                // in case of junk data, if the source is flexable we'll continue processing bundles
                // until maxErrors is reached
                if (bundleErrors++ < maxErrors) {
                    log.warn("bundleErrors:" + bundleErrors + " is less than max errors: " + maxErrors + ", skipping this bundle");
                    return;
                } else {
                    throw new RuntimeException("Invalid bundle: " + bundle + " unable to read TimeField due to NumberFormatException");
                }
            }
            bench.addEvents(BENCH.UNITS, 1);
            bench.addEvents(BENCH.TIME, bundleTime >> 8);
            processPath(bundle, pathIndex.getValueByIndex(unit));
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        processed.incrementAndGet();
        bench.addEvents(BENCH.LOCAL, 1);
        checkBench();
    }

    private long getBundleTime(Bundle bundle) {
        long bundleTime = JitterClock.globalTime();
        if (timeField != null) {
            ValueObject vo = bundle.getValue(bundle.getFormat().getField(timeField.getField()));
            if (vo == null) {
                if (log.isDebugEnabled()) {
                    log.debug("missing time " + timeField.getField() + " in [" + bundle.getCount() + "] --> " + bundle);
                }
            } else {
                bundleTime = timeField.toUnix(vo);
            }
        }
        return bundleTime;
    }

    /**
     * Processor interface this is where packets and rules are finally executed
     * locally.
     */
    private void processPath(Bundle bundle, PathElement[] path) {
        try {
            TreeMapState ps = new TreeMapState(this, tree, path, bundle);
            processNodes.addAndGet(ps.touched());
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        bench.addEvents(BENCH.RULES, 1);
    }

    /**
     * print benchmark data to log
     */
    protected void checkBench() {
        synchronized (bench) {
            if (bench.hasElapsed(printinterval)) {
                long time = System.currentTimeMillis() - startTime;
                long proc = processed.get();
                // prevent multiple Hydra threads from competing to change
                // streamXX,lastTime vars
                if (benchCalls.getAndIncrement() % 20 == 0 && stats) {
                    long streamCounts = streamReadCount.getAndSet(0);
                    long streamTotals = streamReadTotal.addAndGet(streamCounts) / (1024 * 1024);
                    long mark = JitterClock.globalTime();
                    long streamRate = (streamCounts * 1000L) / (mark - lastHeaderTime.getAndSet(mark));
                    log.info("tread tmap  input proc  rules  nodes bundles cache..hit% dbs   " + "mem   bundleTime [" + streamCounts + "," + streamRate + "/s," + streamTotals + "MM]");
                }
                long benchtime = bench.getEventCount(BENCH.TIME);
                long benchlocal = bench.getEventCount(BENCH.UNITS);
                long streamRate = bench.getEventRate(BENCH.STREAM);
                bench.mark();

                long avg_t = benchtime / Math.max(1, benchlocal) << 8;
                long time_write_map = mapWriteTime.getAndSet(0);
                long time_read_wait = streamWaitime.getAndSet(0);

                Snapshot snap = new Snapshot();
                snap.streamRate = streamRate;
                snap.mapWriteTime = benchlocal > 0 ? time_write_map / benchlocal : time_write_map;
                snap.streamWaitTime = (benchlocal > 0 ? time_read_wait / benchlocal : time_read_wait);
                snap.localPacketRate = bench.getEventRate(BENCH.LOCAL);
                snap.ruleProcessRate = bench.getEventRate(BENCH.RULES);
                snap.nodesUpdated = processNodes.getAndSet(0);
                snap.totalPackets = proc;
                snap.treeCacheSize = tree.getCacheSize();
                snap.treeCacheHitRate = tree.getCacheHitRate();
                snap.treeDbCount = tree.getDBCount();
                snap.freeMemory = Runtime.getRuntime().freeMemory() / 1024L / 1024L;
                snap.averageTimestamp = date.format(avg_t);
                snap.runningTime = time;
                mapstats.setSnapshot(snap);

                if (!stats) {
                    return;
                }
                StringBuilder msg = new StringBuilder();
                msg.append(pad(snap.streamWaitTime, 6));
                msg.append(pad(snap.mapWriteTime, 6));
                msg.append(pad(snap.streamRate, 6));
                msg.append(pad(snap.localPacketRate, 6));
                msg.append(pad(snap.ruleProcessRate, 7));
                msg.append(pad(snap.nodesUpdated, 6));
                msg.append(pad(snap.totalPackets, 8));
                msg.append(pad(snap.treeCacheSize, 6));
                msg.append(pad(percent.format(snap.treeCacheHitRate), 6));
                msg.append(pad(snap.treeDbCount, 6));
                msg.append(pad(snap.freeMemory, 6));
                msg.append(pad(snap.averageTimestamp, 14));
                log.info(msg.toString());
            }
        }
    }

    /**
     * number right pad utility for log data
     */
    private static String pad(long v, int chars) {
        String sv = Long.toString(v);
        String[] opt = new String[]{"K", "M", "B", "T"};
        DecimalFormat[] dco = new DecimalFormat[]{new DecimalFormat("0.00"), new DecimalFormat("0.0"), new DecimalFormat("0")};
        int indx = 0;
        double div = 1000d;
        outer:
        while (sv.length() > chars - 1 && indx < opt.length) {
            for (DecimalFormat dc : dco) {
                sv = dc.format(v / div).concat(opt[indx]);
                if (sv.length() <= chars - 1) {
                    break outer;
                }
            }
            div *= 1000;
            indx++;
        }
        return Strings.padright(sv, chars);
    }

    /**
     * string right pad utility for log data
     */
    private static String pad(String s, int chars) {
        return Strings.padright(s, chars);
    }

    @Override
    public void send(Bundle bundle) {
        long markBefore = System.nanoTime();
        streamWaitime.addAndGet(markBefore - lastBundleTime.getAndSet(markBefore));
        processBundle(bundle, root);
        long markAfter = System.nanoTime();
        mapWriteTime.addAndGet(markAfter - markBefore);
        streamReadCount.incrementAndGet();
        bench.addEvents(BENCH.STREAM, 1);
        lastBundleTime.set(markAfter);
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override
    public void sendComplete() {
        try {
            boolean doPost = false;
            if (post != null) {
                int sample = 0;
                if (postRate > 1) {
                    File sampleFile = new File("post.sample");
                    if (Files.isFileReadable("post.sample")) {
                        try {
                            sample = Integer.parseInt(Bytes.toString(Files.read(sampleFile)));
                            sample = (sample + 1) % postRate;
                        } catch (NumberFormatException ignored) {

                        }
                    }
                    doPost = (sample == 0);
                    Files.write(sampleFile, Bytes.toBytes(Integer.toString(sample)), false);
                } else {
                    doPost = true;
                }
                if (doPost) {
                    log.warn("post-chain: " + post);
                    processBundle(new KVBundle(), post);
                    processed.incrementAndGet();
                } else {
                    log.warn("skipping post-chain: " + post +
                             ". Sample rate is " + sample + " out of " + postRate);
                }
            }
            if (outputs != null) {
                for (PathOutput output : outputs) {
                    log.warn("output: " + output);
                    output.exec(tree);
                }
            }
            // turn off live queries
            if (liveQueryServer != null) {
                liveQueryServer.close();
            }
            // disable web interface
            boolean doValidate;
            switch (validateTree) {
                case ALL:
                    doValidate = true;
                    break;
                case POST:
                    doValidate = doPost;
                    break;
                case NONE:
                default:
                    doValidate = false;
            }
            // close storage
            log.info("[close] closing tree storage");
            CloseOperation closeOperation = CloseOperation.NONE;
            if (doValidate) {
                closeOperation = repairTree ? CloseOperation.REPAIR : CloseOperation.TEST;
            }
            tree.close(false, closeOperation);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void sourceError(Throwable err) {
        // TODO
    }
}

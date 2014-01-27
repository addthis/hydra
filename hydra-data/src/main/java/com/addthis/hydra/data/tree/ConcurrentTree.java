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
package com.addthis.hydra.data.tree;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.concurrentlinkedhashmap.EvictionMediator;
import com.addthis.basis.concurrentlinkedhashmap.MediatedEvictionConcurrentHashMap;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Meter;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.IPageDB.Range;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.store.kv.PagedKeyValueStore;
import com.addthis.hydra.store.skiplist.SkipListCache;
import com.addthis.hydra.store.util.MeterFileLogger;
import com.addthis.hydra.store.util.MeterFileLogger.MeterDataSource;
import com.addthis.hydra.store.util.NamedThreadFactory;
import com.addthis.hydra.store.util.Raw;

import com.google.common.util.concurrent.AtomicDouble;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConcurrentTree implements DataTree, MeterDataSource {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentTree.class);

    static int defaultKeyValueStoreType = Parameter.intValue("hydra.tree.concurrent.kvstore.type", 1);
    // number of background deletion threads
    static int defaultNumDeletionThreads = Parameter.intValue("hydra.tree.clean.threads", 1);
    // sleep interval of deletion threads in between polls of deletion queue
    static int deletionThreadSleepMillis = Parameter.intValue("hydra.tree.clean.interval", 10);
    static final boolean trashDebug = Parameter.boolValue("hydra.tree.trash.debug", false);

    private static final AtomicInteger scopeGenerator = new AtomicInteger();

    private final String scope = "ConcurrentTree" + Integer.toString(scopeGenerator.getAndIncrement());

    public static enum METERTREE {
        CACHE_HIT, CACHE_MISS, NODE_PUT, NODE_CREATE, NODE_DELETE, SOURCE_MISS
    }

    static final String keyCacheGet = METERTREE.CACHE_HIT.toString();
    static final String keyCacheMiss = METERTREE.CACHE_MISS.toString();

    private final File root;
    private final File idFile;
    private final IPageDB<DBKey, ConcurrentTreeNode> source;
    private final ConcurrentTreeNode treeRootNode;
    private final ConcurrentTreeNode treeTrashNode;
    private final AtomicInteger nextDBID;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean readonly;
    private final Meter<METERTREE> meter;
    private final MeterFileLogger logger;
    private final AtomicDouble cacheHitRate = new AtomicDouble(0.0);
    private final boolean meterLoggerEnabled = true;
    private final MediatedEvictionConcurrentHashMap<CacheKey, ConcurrentTreeNode> cache;
    private final ScheduledExecutorService deletionThreadPool;

    @GuardedBy("treeTrashNode")
    private Range<DBKey, ConcurrentTreeNode> trashIterator;

    @SuppressWarnings("unused")
    final Gauge<Integer> treeTrashNodeCount = Metrics.newGauge(SkipListCache.class,
            "treeTrashNodeCount", scope,
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return treeTrashNode == null ? -1 : treeTrashNode.getNodeCount();
                }
            });

    @SuppressWarnings("unused")
    final Gauge<Long> treeTrashHitsCount = Metrics.newGauge(SkipListCache.class,
            "treeTrashHitsCount", scope,
            new Gauge<Long>() {
                @Override
                public Long value() {
                    return treeTrashNode == null ? -1 : treeTrashNode.getCounter();
                }
            });


    /**
     * convert meter enums to sensible short names for reporting
     */
    protected static String shortName(String meterName) {
        StringBuilder sb = new StringBuilder();
        for (String s : Strings.split(meterName.toString(), "_")) {
            sb.append(s.length() >= 2 ? s.substring(0, 2) : s + "_");
        }
        return sb.toString();
    }

    public static class Builder {

        // Required parameters
        protected final File root;
        protected final boolean readonly;

        // Optional parameters - initialized to default values;
        protected int numDeletionThreads = defaultNumDeletionThreads;
        protected int kvStoreType = defaultKeyValueStoreType;
        protected int cleanQSize = TreeCommonParameters.cleanQMax;
        protected int maxCache = TreeCommonParameters.maxCacheSize;
        protected int maxPageSize = TreeCommonParameters.maxPageSize;

        public Builder(File root, boolean readonly) {
            this.root = root;
            this.readonly = readonly;
        }

        public Builder numDeletionThreads(int val) {
            numDeletionThreads = val;
            return this;
        }

        public Builder kvStoreType(int val) {
            kvStoreType = val;
            return this;
        }

        public Builder cleanQSize(int val) {
            cleanQSize = val;
            return this;
        }

        public Builder maxCacheSize(int val) {
            maxCache = val;
            return this;
        }

        public Builder maxPageSize(int val) {
            maxPageSize = val;
            return this;
        }

        public ConcurrentTree build() throws Exception {
            return new ConcurrentTree(root, readonly,
                    numDeletionThreads, kvStoreType, cleanQSize, maxCache, maxPageSize);
        }

    }

    private ConcurrentTree(File root, boolean readonly,
            int numDeletionThreads, int kvStoreType,
            int cleanQSize, int maxCacheSize, int maxPageSize) throws Exception {
        //Only attempt mkdirs if we are not readonly. Theoretically should not be needed, but guarding here
        // prevent logic leak created by transient file detection issues. Regardless, while in readonly, we should
        // certainly not be attempting to create directories.
        if (!(root.isDirectory() || (!readonly && root.mkdirs()))) {
            throw new IOException("Unable to open or create root directory '" + root + "'");
        }
        this.root = root;
        this.readonly = readonly;
        long start = System.currentTimeMillis();

        // setup metering
        meter = new Meter<>(METERTREE.values());
        for (METERTREE m : METERTREE.values()) {
            meter.addCountMetric(m, m.toString());
        }

        // create meter logging thread
        if (TreeCommonParameters.meterLogging > 0 && meterLoggerEnabled) {
            logger = new MeterFileLogger(this, root, "tree-metrics", TreeCommonParameters.meterLogging, 100000);
        } else {
            logger = null;
        }
        source = new PageDB.Builder<>(root, ConcurrentTreeNode.class, maxPageSize, maxCacheSize)
                .readonly(readonly).kvStoreType(kvStoreType).build();
        source.setCacheMem(TreeCommonParameters.maxCacheMem);
        source.setPageMem(TreeCommonParameters.maxPageMem);
        source.setMemSampleInterval(TreeCommonParameters.memSample);
        // create cache
        cache = new MediatedEvictionConcurrentHashMap.
                Builder<CacheKey, ConcurrentTreeNode>().
                mediator(new CacheMediator()).
                maximumWeightedCapacity(cleanQSize).build();

        // get stored next db id
        idFile = new File(root, "nextID");
        if (idFile.exists() && idFile.isFile() && idFile.length() > 0) {
            nextDBID = new AtomicInteger(Integer.parseInt(Bytes.toString(Files.read(idFile))));
        } else {
            nextDBID = new AtomicInteger(1);
        }

        // get tree root
        ConcurrentTreeNode dummyRoot = ConcurrentTreeNode.getTreeRoot(this);
        if (isReadOnly()) {
            treeRootNode = dummyRoot.getNode("root");
            if (treeRootNode == null) {
                throw new RuntimeException("missing root in readonly tree");
            }
            treeTrashNode = null;
            deletionThreadPool = null;
        } else {
            treeRootNode = (ConcurrentTreeNode) dummyRoot.getOrCreateEditableNode("root");
            treeTrashNode = (ConcurrentTreeNode) dummyRoot.getOrCreateEditableNode("trash");
            treeTrashNode.requireNodeDB();
            deletionThreadPool = Executors.newScheduledThreadPool(numDeletionThreads,
                    new NamedThreadFactory(scope + "-deletion-", true));

            for (int i = 0; i < numDeletionThreads; i++) {
                deletionThreadPool.scheduleAtFixedRate(new BackgroundDeletionTask(),
                        i,
                        deletionThreadSleepMillis,
                        TimeUnit.MILLISECONDS);
            }
        }

        long openTime = System.currentTimeMillis() - start;
        log.info("dir=" + root +
                 (log.isDebugEnabled() ? " root=" + treeRootNode + " trash=" + treeTrashNode : "")
                 + " cache=" + TreeCommonParameters.cleanQMax + " nextdb=" + nextDBID
                 + " openms=" + openTime);
    }

    public ConcurrentTree(File root, boolean readonly) throws Exception {
        this(root, readonly, defaultNumDeletionThreads,
                defaultKeyValueStoreType, TreeCommonParameters.cleanQMax,
                TreeCommonParameters.maxCacheSize, TreeCommonParameters.maxPageSize);
    }

    private class CacheMediator implements EvictionMediator<CacheKey, ConcurrentTreeNode> {

        @Override
        public boolean onEviction(CacheKey key, ConcurrentTreeNode value) {
            boolean evict = value.trySetEviction();
            if (evict) {
                try {
                    if (!value.isDeleted() && value.isChanged()) {
                        source.put(key.dbkey(), value);
                    }
                } finally {
                    value.evictionComplete();
                }
            }
            return evict;
        }
    }

    private void logException(String message, Exception ex) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        ex.printStackTrace(printWriter);
        log.warn(message + " : " + result.toString());
    }

    public void meter(METERTREE meterval) {
        meter.inc(meterval);
    }

    public boolean isReadOnly() {
        return readonly;
    }

    protected int getNextNodeDB() {
        return nextDBID.incrementAndGet();
    }

    private static boolean setLease(final ConcurrentTreeNode node, final boolean lease) {
        return (!lease || node.tryLease());
    }

    protected ConcurrentTreeNode getNode(final ConcurrentTreeNode parent, final String child,
            final boolean lease) {
        Integer nodedb = parent.nodeDB();
        if (nodedb == null) {
            if (log.isTraceEnabled()) log.trace("[node.get] " + parent + " --> " + child + " NOMAP --> null");
            return null;
        }
        CacheKey key = new CacheKey(nodedb, child);
        DBKey dbkey = key.dbkey();

        /**
         * (1) First check the cache for the (key, value) pair. If the value
         * is found and the value is successfully leased then return it.
         * (2) Otherwise the value is not found in the cache. Check the backing store
         * for the value. If the value is not found in the backing store then (3) return
         * null. Otherwise (4) if the value is found in the backing store and
         * successfully inserted into the cache and the value is leased then
         * return the value. If all of these steps are unsuccessful then repeat.
         */

        while (true) {
            ConcurrentTreeNode node = cache.get(key);
            if (node != null) {
                if (node.isDeleted()) {
                    cache.remove(key, node);
                } else if (setLease(node, lease)) {
                    reportCacheHit();
                    return node; // (1)
                }
            } else {// (2)
                reportCacheMiss();
                node = source.get(dbkey);

                if (node == null) {
                    meter.inc(METERTREE.SOURCE_MISS);
                    return null; // (3)
                }

                if (node.isDeleted()) {
                    source.remove(dbkey);
                } else {
                    node.initIfDecoded(this, dbkey, key.name);

                    ConcurrentTreeNode prev = cache.putIfAbsent(key, node);
                    if (prev == null) {
                        node.reactivate();
                        if (setLease(node, lease)) {
                            return node; // (4)
                        }
                    }
                }
            }
        }
    }

    protected ConcurrentTreeNode getOrCreateNode(final ConcurrentTreeNode parent, final String child,
            final DataTreeNodeInitializer creator) {
        parent.requireNodeDB();
        CacheKey key = new CacheKey(parent.nodeDB(), child);
        DBKey dbkey = key.dbkey();
        ConcurrentTreeNode newNode = null;

        while (true) {
            ConcurrentTreeNode node = cache.get(key);
            if (node != null) {
                if (node.isDeleted()) {
                    cache.remove(key, node);
                } else if (setLease(node, true)) {
                    reportCacheHit();
                    return node;
                }
            } else {
                reportCacheMiss();
                node = source.get(dbkey);

                if (node != null) {
                    if (node.isDeleted()) {
                        source.remove(dbkey);
                    } else {
                        node.initIfDecoded(this, dbkey, key.name);
                        ConcurrentTreeNode prev = cache.putIfAbsent(key, node);
                        if (prev == null) {
                            node.reactivate();
                            if (setLease(node, true)) {
                                return node;
                            }
                        }
                    }
                } else { // create a new node
                    if (newNode == null) {
                        newNode = new ConcurrentTreeNode();
                        newNode.init(this, dbkey, key.name);
                        newNode.tryLease();
                        newNode.markChanged();
                        if (creator != null) {
                            creator.onNewNode(newNode);
                        }
                    }
                    node = newNode;
                    if (cache.putIfAbsent(key, node) == null) {
                        /**
                         * We must insert the new node into the external storage
                         * because our iterators traverse this data
                         * structure to search for nodes.
                         */
                        source.put(dbkey, node);
                        parent.updateNodeCount(1);
                        return node;
                    }
                }
            }
        }
    }

    protected boolean deleteNode(final ConcurrentTreeNode parent, final String child) {
        if (log.isTraceEnabled()) log.trace("[node.delete] " + parent + " --> " + child);
        Integer nodedb = parent.nodeDB();
        if (nodedb == null) {
            if (log.isDebugEnabled())
                log.debug("parent has no children on delete : " + parent + " --> " + child);
            return false;
        }
        CacheKey key = new CacheKey(nodedb, child);
        while (true) {
            ConcurrentTreeNode node = getNode(parent, child, false);
            if (node != null) {
                if (node.getLeaseCount() == -1) {
                    continue;
                }
                node.markDeleted();
                source.remove(key.dbkey());
                cache.remove(key);
                parent.updateNodeCount(-1);
                if (node.hasNodes() && !node.isAlias()) {
                    markForChildDeletion(node);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    protected void markForChildDeletion(final ConcurrentTreeNode node) {
        /*
         * only put nodes in the trash if they have children because they've
         * otherwise already been purged from backing store by release() in the
         * TreeCache.
         */
        assert (node.hasNodes());
        assert (!node.isAlias());
        int nodeDB = treeTrashNode.nodeDB();
        int next = treeTrashNode.incrementNodeCount();
        DBKey key = new DBKey(nodeDB, Raw.get(Bytes.toBytes(next)));
        source.put(key, node);
        if (log.isTraceEnabled()) log.trace("[trash.mark] " + next + " --> " + treeTrashNode);
    }

    @SuppressWarnings("unchecked")
    protected Range<DBKey, ConcurrentTreeNode> fetchNodeRange(int db) {
        return source.range(new DBKey(db), new DBKey(db + 1));
    }

    @SuppressWarnings({"unchecked", "unused"})
    protected Range<DBKey, ConcurrentTreeNode> fetchNodeRange(int db, String from) {
        return source.range(new DBKey(db, Raw.get(from)), new DBKey(db + 1));
    }

    @SuppressWarnings("unchecked")
    protected Range<DBKey, ConcurrentTreeNode> fetchNodeRange(int db, String from, String to) {
        return source.range(new DBKey(db, Raw.get(from)),
                to == null ? new DBKey(db+1, (Raw)null) : new DBKey(db, Raw.get(to)));
    }

    public ConcurrentTreeNode getRootNode() {
        return treeRootNode;
    }

    /**
     * Package-level visibility is for testing purposes only.
     */
    void waitOnDeletions() {
        shutdownDeletionThreadPool();
        processTrash();
    }

    /**
     * Package-level visibility is for testing purposes only.
     */
    ConcurrentMap<CacheKey, ConcurrentTreeNode> getCache() {
        return cache;
    }

    private void shutdownDeletionThreadPool() {
        if (deletionThreadPool == null)
            return;

        deletionThreadPool.shutdown();

        try {
            if (!deletionThreadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Waiting on outstanding node deletions to complete.");
                deletionThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void sync() throws IOException {
        if (isReadOnly()) {
            return;
        }
        log.debug("[sync] start");
        for (ConcurrentTreeNode node : cache.values()) {
            if (!node.isDeleted() && node.isChanged()) {
                source.put(node.dbkey, node);
            }
        }
        log.debug("[sync] end nextdb={}", nextDBID);
        Files.write(idFile, Bytes.toBytes(nextDBID.toString()), false);
    }

    @Override
    public int getDBCount() {
        return nextDBID.get();
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    public double getCacheHitRate() {
        if (logger == null) {
            getIntervalData();
        }
        return cacheHitRate.get();
    }

    /**
     * Close the tree.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     */
    @Override
    public void close(boolean cleanLog, CloseOperation operation) {
        if (!closed.compareAndSet(false, true)) {
            log.trace("already closed");
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("closing " + this);
        }
        if (!isReadOnly()) {
            try {
                waitOnDeletions();
            } catch (Exception e) {
                log.warn("", e);
            }
            if (treeRootNode != null) {
                treeRootNode.markChanged();
                treeRootNode.release();
                if (treeRootNode.getLeaseCount() != 0) {
                    log.warn("invalid root state on shutdown : " + treeRootNode);
                }
            }
            if (treeTrashNode != null) {
                treeTrashNode.markChanged();
                treeTrashNode.release();
            }
            try {
                sync();
            } catch (Exception e) {
                log.warn("", e);
            }
        }
        if (source != null) {
            try {
                int status = source.close(cleanLog, operation);
                if (status != 0) {
                    Runtime.getRuntime().halt(status);
                }
            } catch (Exception ex) {
                log.warn("", ex);
            }
        }
        if (logger != null) {
            logger.terminate();
        }
    }


    @Override
    public void close() {
        close(false, CloseOperation.NONE);
    }

    @Override
    public Map<String, Long> getIntervalData() {
        Map<String, Long> mark = meter.mark();
        Long gets = mark.get(keyCacheGet);
        Long miss = mark.get(keyCacheMiss);
        if (gets == null || miss == null || miss == 0) {
            cacheHitRate.set(0);
        } else {
            cacheHitRate.set(1.0d - ((miss * 1.0d) / ((gets + miss) * 1.0d)));
        }
        return mark;
    }

    protected void reportCacheHit() {
        meter.inc(METERTREE.CACHE_HIT);
    }

    protected void reportCacheMiss() {
        meter.inc(METERTREE.CACHE_MISS);
    }

    protected static class CacheKey {

        private final int hc;
        private final int db;
        private final String name;
        private volatile DBKey dbkey;

        protected CacheKey(int db, String name) {
            int hash = Math.abs(db + name.hashCode());
            this.db = db;
            this.name = name;
            if (hash == Integer.MIN_VALUE) {
                hash = Integer.MAX_VALUE;
            }
            hc = hash;
        }

        protected DBKey dbkey() {
            if (dbkey == null) {
                dbkey = new DBKey(db, Raw.get(name));
            }
            return dbkey;
        }

        @Override
        public boolean equals(Object key) {
            if (!(key instanceof CacheKey)) {
                return false;
            }
            CacheKey ck = (CacheKey) key;
            return ck.db == db && ck.name.equals(name);
        }

        @Override
        public int hashCode() {
            return hc;
        }
    }

    @Override
    public String toString() {
        return "Tree@" + root;
    }

    @Override
    public DataTreeNode getNode(String name) {
        return getRootNode().getNode(name);
    }

    @Override
    public DataTreeNode getLeasedNode(String name) {
        return getRootNode().getLeasedNode(name);
    }

    @Override
    public DataTreeNode getOrCreateNode(String name, DataTreeNodeInitializer init) {
        return getRootNode().getOrCreateNode(name, init);
    }

    @Override
    public boolean deleteNode(String node) {
        return getRootNode().deleteNode(node);
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator() {
        ConcurrentTreeNode rootNode = getRootNode();
        if (rootNode != null) {
            return getRootNode().getIterator();
        }
        return null;
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String begin) {
        return getRootNode().getIterator(begin);
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String from, String to) {
        return getRootNode().getIterator(from, to);
    }

    @Override
    public Iterator<DataTreeNode> iterator() {
        return getRootNode().iterator();
    }

    @Override
    public String getName() {
        return getRootNode().getName();
    }

    @Override
    public DataTree getTreeRoot() {
        return this;
    }

    @Override
    public int getNodeCount() {
        return getRootNode().getNodeCount();
    }

    @Override
    public long getCounter() {
        return getRootNode().getCounter();
    }

    @Override
    public void incrementCounter() {
        getRootNode().incrementCounter();
    }

    @Override
    public long incrementCounter(long val) {
        return getRootNode().incrementCounter(val);
    }

    @Override
    public void writeLock() {
        getRootNode().writeLock();
    }

    @Override
    public void writeUnlock() {
        getRootNode().writeUnlock();
    }

    @Override
    public void setCounter(long val) {
        getRootNode().setCounter(val);
    }

    @Override
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path) {
        getRootNode().updateChildData(state, path);
    }

    @Override
    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew) {
        getRootNode().updateParentData(state, child, isnew);
    }

    @Override
    public boolean aliasTo(DataTreeNode target) {
        throw new RuntimeException("root node cannot be an alias");
    }

    @Override
    public void lease() {
        getRootNode().lease();
    }

    @Override
    public void release() {
        getRootNode().release();
    }

    @Override
    public DataTreeNodeActor getData(String key) {
        return getRootNode().getData(key);
    }

    @Override
    public Map<String, TreeNodeData> getDataMap() {
        return getRootNode().getDataMap();
    }

    protected static String keyName(DBKey dbkey) {
        try {
            return new String(dbkey.key(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            log.warn("Could not decode the following dbkey bytes into a string: " +
                     Arrays.toString(dbkey.key()));
            return null;
        }
    }

    /**
     * Recursively delete all the children of the input node.
     *
     * @param rootNode
     */
    void deleteSubTree(ConcurrentTreeNode rootNode) {
        int nodeDB = rootNode.nodeDB();
        Range<DBKey, ConcurrentTreeNode> range = fetchNodeRange(nodeDB);
        try {
            while (range.hasNext()) {
                Map.Entry<DBKey, ConcurrentTreeNode> entry = range.next();
                ConcurrentTreeNode next = entry.getValue();

                if (next.hasNodes() && !next.isAlias()) {
                    deleteSubTree(next);
                }
                String name = entry.getKey().rawKey().toString();
                CacheKey key = new CacheKey(nodeDB, name);
                ConcurrentTreeNode cacheNode = cache.remove(key);
                /* Mark the node as deleted so that it will not be
                 * pushed to disk when removed from the eviction queue.
                 */
                if (cacheNode != null) {
                    cacheNode.markDeleted();
                }
            }
        } finally {
            range.close();
        }
        source.remove(new DBKey(nodeDB), new DBKey(nodeDB + 1), false);
    }

    private Map.Entry<DBKey, ConcurrentTreeNode> nextTrashNode() {
        synchronized (treeTrashNode) {
            if (trashIterator == null) {
                return recreateTrashIterator();
            }else if (trashIterator.hasNext()) {
                return trashIterator.next();
            } else {
                return recreateTrashIterator();
            }
        }
    }

    @GuardedBy("treeTrashNode")
    private Map.Entry<DBKey, ConcurrentTreeNode> recreateTrashIterator() {
        if (trashIterator != null) {
            trashIterator.close();
        }
        trashIterator = fetchNodeRange(treeTrashNode.nodeDB());
        if (trashIterator.hasNext()) {
            return trashIterator.next();
        } else {
            trashIterator.close();
            trashIterator = null;
            return null;
        }
    }

    /**
     * Close the trash iterator that was used by the background deletion threads.
     * Open a new iterator to walk through any remaining trash nodes and delete their
     * children. Then perform a range deletion on the trash nodes.
     */
    private void processTrash() {
        synchronized (treeTrashNode) {
            if (trashIterator != null) {
                trashIterator.close();
                trashIterator = null;
            }
        }

        int nodeDB = treeTrashNode.nodeDB();

        Range<DBKey, ConcurrentTreeNode> range = fetchNodeRange(nodeDB);

        while (range.hasNext()) {
            Map.Entry<DBKey, ConcurrentTreeNode> entry = range.next();
            ConcurrentTreeNode node = entry.getValue();
            deleteSubTree(node);
            treeTrashNode.incrementCounter();
        }

        range.close();

        source.remove(new DBKey(nodeDB), new DBKey(nodeDB + 1), false);
    }

    class BackgroundDeletionTask implements Runnable {

        @Override
        public void run() {
            try {
                Map.Entry<DBKey, ConcurrentTreeNode> entry;
                do {
                    entry = nextTrashNode();
                    if (entry != null) {
                        ConcurrentTreeNode node = entry.getValue();
                        deleteSubTree(node);
                        ConcurrentTreeNode prev = source.remove(entry.getKey());
                        if (prev != null) {
                            treeTrashNode.incrementCounter();
                        }
                    }
                }
                while (entry != null && !closed.get());
            } catch (Exception ex) {
                logException("Uncaught exception in concurrent tree background deletion thread", ex);
            }
        }
    }

    /**
     * For testing purposes only.
     */
    ConcurrentTreeNode getTreeTrashNode() {
        return treeTrashNode;
    }

    /**
     * For testing purposes only.
     */
    @SuppressWarnings("unused")
    void testIntegrity() {
        PagedKeyValueStore store = source.getEps();
        if (store instanceof SkipListCache) {
            ((SkipListCache) store).testIntegrity(false);
        }
    }
}

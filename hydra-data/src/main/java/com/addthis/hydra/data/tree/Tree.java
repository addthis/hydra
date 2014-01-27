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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Meter;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.IPageDB.Range;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.store.util.MeterFileLogger;
import com.addthis.hydra.store.util.MeterFileLogger.MeterDataSource;
import com.addthis.hydra.store.util.Raw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO re-add support for bloom filters
 */
public final class Tree implements DataTree, MeterDataSource {

    private static final Logger log = LoggerFactory.getLogger(Tree.class);
    private static final long sluggishOpen = Parameter.longValue("hydra.tree.open.slowwarn", 500);

    public static enum METERTREE {
        CACHE_HIT, CACHE_MISS, NODE_PUT, NODE_CREATE, NODE_DELETE, SOURCE_MISS,
    }

    static final String keyCacheGet = METERTREE.CACHE_HIT.toString();
    static final String keyCacheMiss = METERTREE.CACHE_MISS.toString();

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

    public Tree setDBName(String dbname) {
        this.dbname = dbname;
        return this;
    }

    /**
     * has deferred init() to allow for threaded allocation in Oracle
     */
    public Tree(File root, boolean readonly, boolean init) throws Exception {
        //Only attempt mkdirs if we are not readonly. Theoretically should not be needed, but guarding here
        // prevent logic leak created by transient file detection issues. Regardless, while in readonly, we should
        // certainly not be attempting to create directories.
        if (!(root.isDirectory() || (!readonly && root.mkdirs()))) {
            throw new IOException("Unable to open or create root directory '" + root + "'");
        }
        this.root = root;
        this.dbname = "db.key";
        this.readonly = readonly;
        if (init) {
            init();
        }
    }

    private void init() {
        try {
            _init();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private synchronized void _init() throws Exception {
        long start = System.currentTimeMillis();
        if (source != null) {
            if (log.isTraceEnabled()) {
                log.trace("init called more than once");
            } else if (log.isDebugEnabled()) {
                log.debug("init called more than once");
            }
            return;
        }
        // setup metering
        meter = new Meter<METERTREE>(METERTREE.values());
        for (METERTREE m : METERTREE.values()) {
            meter.addCountMetric(m, m.toString());
        }
        // create meter logging thread
        if (TreeCommonParameters.meterLogging > 0 && meterLoggerEnabled) {
            logger = new MeterFileLogger(this, root, "tree-metrics", TreeCommonParameters.meterLogging, 100000);
        }
        source = new PageDB<>(root, TreeNode.class, TreeCommonParameters.maxPageSize,
                TreeCommonParameters.maxCacheSize, readonly);
        source.setCacheMem(TreeCommonParameters.maxCacheMem);
        source.setPageMem(TreeCommonParameters.maxPageMem);
        source.setMemSampleInterval(TreeCommonParameters.memSample);
        // create cache array
        cache = new HashMap[TreeCommonParameters.cacheShards];
        for (int i = 0; i < TreeCommonParameters.cacheShards; i++) {
            cache[i] = new HashMap<>();
        }
        // get stored next db id
        idFile = new File(root, "nextID");
        if (idFile.exists() && idFile.isFile() && idFile.length() > 0) {
            nextDBID = new AtomicInteger(Integer.parseInt(Bytes.toString(Files.read(idFile))));
        } else {
            nextDBID = new AtomicInteger(1);
        }
        // get tree root
        TreeNode dummyRoot = TreeNode.getTreeRoot(this);
        if (isReadOnly()) {
            treeRootNode = dummyRoot.getNode("root");
            if (treeRootNode == null) {
                throw new RuntimeException("missing root in readonly tree");
            }
        } else {
            treeRootNode = (TreeNode) dummyRoot.getOrCreateEditableNode("root");
            treeTrashNode = (TreeNode) dummyRoot.getOrCreateEditableNode("trash");
            treeTrashNode.requireNodeDB();
            if (TreeCommonParameters.trashInterval > 0) {
                trashman = new TrashMan(TreeCommonParameters.trashInterval);
                trashman.start();
            }
        }
        long openTime = System.currentTimeMillis() - start;
        if (log.isDebugEnabled() || openTime > sluggishOpen) {
            log.warn("dir=" + root +
                     (log.isDebugEnabled() ? " root=" + treeRootNode + " trash=" + treeTrashNode : "")
                     + " cache=" + TreeCommonParameters.cleanQMax + " nextdb=" + nextDBID
                     + " shards=" + TreeCommonParameters.cacheShards + " openms=" + openTime);
        }
    }

    private final File root;
    private File idFile;
    private String dbname;
    private IPageDB<DBKey, TreeNode> source;
    private TreeNode treeRootNode;
    private TreeNode treeTrashNode;
    private AtomicInteger nextDBID;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private boolean readonly;
    private Meter<METERTREE> meter;
    private MeterFileLogger logger;
    private double cacheHitRate;
    private ChangeLogWriter changeLog;
    private boolean meterLoggerEnabled = true;
    private HashMap<CacheKey, TreeNode> cache[];
    private final Deque<TreeNode> cleanQ = new ArrayDeque<>(TreeCommonParameters.cleanQMax);
    private final AtomicInteger cacheSize = new AtomicInteger();
    private TrashMan trashman;
    private final ReentrantReadWriteLock trashLock = new ReentrantReadWriteLock();

    public void meter(METERTREE meterval) {
        meter.inc(meterval);
    }

    public boolean isReadOnly() {
        return readonly;
    }

    public ClosableIterator<Map.Entry<DBKey, TreeNode>> iterateChangeLog(File log) throws FileNotFoundException {
        return new ChangeLogIterator(log);
    }

    public void setChangeLog(File newlog, int keyBuffer) {
        if (changeLog != null) {
            changeLog.close();
        }
        try {
            changeLog = new ChangeLogWriter(newlog, keyBuffer);
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    public int getCacheSize() {
        return cacheSize.get();
    }

    @Override
    public int getDBCount() {
        return nextDBID.get();
    }

    protected int getNextNodeDB() {
        return nextDBID.incrementAndGet();
    }

    @Override
    public double getCacheHitRate() {
        if (logger == null) {
            getIntervalData();
        }
        return cacheHitRate;
    }

    protected TreeNode getNode(final TreeNode parent, final String child, final boolean lease) {
        Integer nodedb = parent.nodeDB();
        if (nodedb == null) {
            if (log.isTraceEnabled()) log.trace("[node.get] " + parent + " --> " + child + " NOMAP --> null");
            return null;
        }
        TreeNode node = null;
        CacheKey key = new CacheKey(nodedb, child);
        HashMap<CacheKey, TreeNode> map = cache[key.hashCode() % cache.length];
        synchronized (map) {
            node = map.get(key);
            if (node == null) {
                reportCacheMiss();
                node = sourceGet(key.dbkey());
                if (node != null && node.getAndClearDecoded()) {
                    node.init(this, key.dbkey(), child);
                } else {
                    meter.inc(METERTREE.SOURCE_MISS);
                }
            } else {
                reportCacheHit();
            }
            if (lease && node != null) {
                node.lease();
            }
        }
        if (log.isTraceEnabled()) log.trace("[node.get] " + parent + " --> " + child + " --> " + node);
        return node;
    }

    protected TreeNode getOrCreateNode(final TreeNode parent, final String child, final DataTreeNodeInitializer creator) {
        parent.requireNodeDB();
        CacheKey key = new CacheKey(parent.nodeDB(), child);
        HashMap<CacheKey, TreeNode> map = cache[key.hashCode() % cache.length];
        TreeNode node;
        boolean create = false;
        synchronized (map) {
            node = map.get(key);
            if (node == null) {
                reportCacheMiss();
                DBKey dbkey = key.dbkey();
                node = sourceGet(dbkey);
                if (node != null) {
                    if (node.getAndClearDecoded()) {
                        node.init(this, dbkey, key.name);
                    }
                } else {
                    create = true;
                    node = new TreeNode();
                    node.init(this, dbkey, key.name);
                    sourcePut(dbkey, node);
                    parent.updateNodeCount(1);
                    meter.inc(METERTREE.NODE_CREATE);
                }
                if (map.put(key, node) == null) {
                    cacheSize.incrementAndGet();
                }
            } else {
                reportCacheHit();
            }
            node.lease();
            if (create) {
                if (creator != null) {
                    creator.onNewNode(node);
                }
                node.markChanged();
            }
        }
        if (log.isTraceEnabled()) log.trace("[node.getOrCreate] " + parent + " --> " + child + " --> " + node);
        return node;
    }

    protected boolean deleteNode(final TreeNode parent, final String child) {
        if (log.isTraceEnabled()) log.trace("[node.delete] " + parent + " --> " + child);
        Integer nodedb = parent.nodeDB();
        if (nodedb == null) {
            if (log.isDebugEnabled()) log.debug("parent has no children on delete : " + parent + " --> " + child);
            return false;
        }
        CacheKey key = new CacheKey(nodedb, child);
        HashMap<CacheKey, TreeNode> map = cache[key.hashCode() % cache.length];
        synchronized (map) {
            TreeNode deleted = map.remove(key);
            if (deleted == null) {
                deleted = sourceGet(key.dbkey());
            } else {
                cacheSize.decrementAndGet();
            }
            if (deleted != null) {
                deleted.markDeleted();
                parent.updateNodeCount(-1);
                // to avoid NPE b/c on sourceGet dbkey is not set on the retrieved object
                sourceDelete(deleted.dbkey != null ? deleted.dbkey : key.dbkey());
                if (deleted.hasNodes() && !deleted.isAlias()) {
                    markForChildDeletion(deleted);
                }
                return true;
            }
            return false;
        }
    }

    protected TreeNode sourceGet(final DBKey key) {
        trashLock.readLock().lock();
        try {
            TreeNode node = source.get(key);
            if (log.isTraceEnabled()) log.trace("[source.get] " + key + " --> " + node);
            return node;
        } finally {
            trashLock.readLock().unlock();
        }
    }

    /**
     * update or remove key from underlying data source.
     * coordinates with changelog, if active.
     */
    protected void sourceDelete(final DBKey key) {
        if (log.isTraceEnabled()) log.trace("[source.del] " + key);
        trashLock.readLock().lock();
        try {
            if (source.remove(key) != null) {
                meter.inc(METERTREE.NODE_DELETE);
            }
        } finally {
            trashLock.readLock().unlock();
        }
        if (changeLog != null) {
            changeLog.write(key, null);
        }
    }

    protected void sourcePut(final DBKey key, final TreeNode value) {
        if (log.isTraceEnabled() || value == treeRootNode) {
            log.warn("[source.put] " + key + " --> " + value);
        }
        if (value == null) {
            throw new NullPointerException("cannot put a null: " + key);
        }
        trashLock.readLock().lock();
        try {
            source.put(key, value);
        } finally {
            trashLock.readLock().unlock();
        }
        meter.inc(METERTREE.NODE_PUT);
        if (changeLog != null) {
            changeLog.write(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    protected Range fetchNodeRange(int db) {
        return source.range(new DBKey(db), new DBKey(db + 1));
    }

    @SuppressWarnings("unchecked")
    protected Range fetchNodeRange(int db, String from) {
        return source.range(new DBKey(db, Raw.get(from)), new DBKey(db + 1));
    }

    @SuppressWarnings("unchecked")
    protected Range fetchNodeRange(int db, String from, String to) {
        return source.range(new DBKey(db, Raw.get(from)), to == null ? new DBKey(db+1, (Raw)null) : new DBKey(db, Raw.get(to)));
    }

    public TreeNode getRootNode() {
        return treeRootNode;
    }

    protected void markForChildDeletion(final TreeNode node) {
        /*
         * only put nodes in the trash if they have children because they've
         * otherwise already been purged from backing store by release() in the
         * TreeCache.
         */
        synchronized (treeTrashNode) {
            int next = treeTrashNode.nodes++;
            sourcePut(new DBKey(treeTrashNode.nodeDB(), Raw.get(Bytes.toBytes(next))), node);
            if (log.isTraceEnabled()) log.trace("[trash.mark] " + next + " --> " + treeTrashNode);
        }
    }

    /** */
    public DataTreeNode findNode(String path[]) {
        return findNodeFrom(treeRootNode, path);
    }

    /** */
    public DataTreeNode findNodeFrom(DataTreeNode node, String path[]) {
        if (node == null) {
            node = treeRootNode;
        }
        int plen = path.length;
        for (int i = 0; i < plen; i++) {
            node = node.getNode(path[i]);
            if (node == null || i == plen - 1) {
                return node;
            }
        }
        return node;
    }

    public void sync() throws IOException {
        if (isReadOnly()) {
            return;
        }
        if (log.isDebugEnabled()) log.debug("[sync] start cleanq=" + cleanQ.size());
        TreeNode clean = null;
        while (cleanQ.size() > 0) {
            clean = cleanQ.removeLast();
            if (clean.getAndClearChanged() && !clean.isDeleted()) {
                sourcePut(clean.dbkey, clean);
            }
        }
        if (log.isDebugEnabled()) log.debug("[sync] end cleanq=" + cleanQ.size() + " nextdb=" + nextDBID);
        Files.write(idFile, Bytes.toBytes(nextDBID.toString()), false);
    }

    private void processTrash(boolean sync) {
        if (isReadOnly() || treeTrashNode.getNodeCount() == 0) {
            return;
        }
        trashLock.writeLock().lock();
        try {
            if (TreeCommonParameters.trashDebug || sync || log.isDebugEnabled()) {
                log.debug("[trash] processing trash from " + treeTrashNode + " @ " + treeTrashNode.getCounter());
            }

            int deleted = deleteTrashChildren(sync ? 0 : TreeCommonParameters.trashMaxTime);

            if (TreeCommonParameters.trashDebug || sync || log.isDebugEnabled()) {
                log.debug("[trash] deleted " + deleted + " nodes");
            }
        } catch (Throwable re) {
            log.warn("[trash] error " + re);
            re.printStackTrace();
        } finally {
            trashLock.writeLock().unlock();
        }
    }

    private int deleteTrashChildren(long maxTime) {
        int deleted = 0;
        int nodedb = treeTrashNode.nodeDB();
        long start = JitterClock.globalTime();
        Range<DBKey, TreeNode> range = source.range(new DBKey(nodedb, Raw.get(Bytes.toBytes((int) treeTrashNode.getCounter()))), new DBKey(nodedb + 1));
        if (range != null) {
            while (range.hasNext()) {
                Entry<DBKey, TreeNode> next = range.next();
                DBKey key = next.getKey();
                int index = Bytes.toInt(key.key());
                if (TreeCommonParameters.trashDebug || log.isTraceEnabled()) {
                    log.trace("[trash.next] " + index + " --> " + key + " --> " + treeTrashNode);
                }
                if (index > treeTrashNode.getCounter()) {
                    log.warn("[trash] stop cleaning on non-contiguous range : " + treeTrashNode.getCounter() + " vs " + index + ". deleted " + deleted);
                    break;
                }
                if (maxTime > 0 && JitterClock.globalTime() - start > maxTime) {
                    if (TreeCommonParameters.trashDebug || log.isDebugEnabled()) {
                        log.info("[trash] stop cleaning on max time. deleted " + deleted);
                    }
                    break;
                }
                TreeNode trash = next.getValue();
                deleted += recursiveChildDelete(trash, 1) + 1;
                sourceDelete(key);
                treeTrashNode.hits++;
            }
            range.close();
        }
        return deleted;
    }

    private int recursiveChildDelete(TreeNode node, int depth) {
        int deleted = 0;
        Integer nodedb = node.nodeDB();
        if (nodedb != null && !node.isAlias()) {
            if (log.isDebugEnabled()) log.debug("[trash." + depth + "] delete children of " + node);
            Range<DBKey, TreeNode> range = fetchNodeRange(nodedb);
            if (range != null) {
                while (range.hasNext()) {
                    Entry<DBKey, TreeNode> next = range.next();
                    DBKey key = next.getKey();
                    TreeNode trash = next.getValue();
                    deleted += recursiveChildDelete(trash, depth + 1) + 1;
                    sourceDelete(key);
                }
                range.close();
            }
        }
        return deleted;
    }

    public void close() {
        close(false, CloseOperation.NONE);
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
            if (trashman != null) {
                trashman.terminate();
            }
            processTrash(true);
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
            } catch (Exception e)  {
                log.warn("", e);
            }
        }
        if (source != null) {
            try {
                source.close(cleanLog, operation);
            } catch (Exception ex)  {
                log.warn("", ex);
            }
        }
        if (logger != null) {
            logger.terminate();
        }
        if (changeLog != null) {
            changeLog.close();
        }
    }

    @Override
    public Map<String, Long> getIntervalData() {
        Map<String, Long> mark = meter.mark();
        Long gets = mark.get(keyCacheGet);
        Long miss = mark.get(keyCacheMiss);
        if (gets == null || miss == null || miss == 0) {
            cacheHitRate = 0;
        } else {
            cacheHitRate = (1.0d - ((miss * 1.0d) / ((gets + miss) * 1.0d)));
        }
        return mark;
    }

    public boolean getMeterLoggerEnabled() {
        return this.meterLoggerEnabled;
    }

    public void setMeterLoggerEnabled(boolean enabled) {
        this.meterLoggerEnabled = enabled;
    }

    protected void reportCacheHit() {
        meter.inc(METERTREE.CACHE_HIT);
    }

    protected void reportCacheMiss() {
        meter.inc(METERTREE.CACHE_MISS);
    }

    protected class CacheKey {

        private int hc;
        private int db;
        private String name;
        private DBKey dbkey;

        protected CacheKey(TreeNode node) {
            this(node.dbkey.id(), node.name);
        }

        protected CacheKey(int db, String name) {
            this.hc = Math.abs(db + name.hashCode());
            this.db = db;
            this.name = name;
            if (hc == Integer.MIN_VALUE) {
                hc = Integer.MAX_VALUE;
            }
        }

        protected synchronized DBKey dbkey() {
            if (dbkey == null) {
                dbkey = new DBKey(db, Raw.get(name));
            }
            return dbkey;
        }

        @Override
        public boolean equals(Object key) {
            CacheKey ck = (CacheKey) key;
            return ck.db == db && ck.name.equals(name);
        }

        @Override
        public int hashCode() {
            return hc;
        }
    }

    protected void addToCleanQueue(TreeNode child) {
        if (log.isTraceEnabled()) log.trace("[cleanq.add] " + child);
        TreeNode clean = doAddToCleanQueue(child);
        removeFromCache(clean);
    }

    private TreeNode doAddToCleanQueue(TreeNode child) {
        TreeNode clean = null;
        synchronized (cleanQ) {
            child.cleanQInc();
            cleanQ.addFirst(child);
            if (cleanQ.size() > TreeCommonParameters.cleanQMax) {
                clean = cleanQ.removeLast();
                if (log.isTraceEnabled()) log.trace("[cleanq.del] " + clean);
                if (!clean.cleanQDec()) {
                    clean = null;
                }
            }
        }
        return clean;
    }

    private void removeFromCache(TreeNode clean) {
        if (clean != null && clean.getLeaseCount() == 0 && !clean.isDeleted() && clean.getAndClearChanged()) {
            sourcePut(clean.dbkey, clean);
            CacheKey key = new CacheKey(clean);
            HashMap<CacheKey, TreeNode> map = cache[key.hashCode() % cache.length];
            synchronized (map) {
                if (map.remove(key) != null) {
                    cacheSize.decrementAndGet();
                }
            }
        }
    }

    class TrashMan extends Thread {

        AtomicBoolean running;
        long interval;

        TrashMan(long interval) {
            this.interval = interval;
            running = new AtomicBoolean(true);
        }

        public void terminate() {
            running.set(false);
            synchronized (this) {
                notifyAll();
            }
            try {
                join();
            } catch (InterruptedException e)  {
                log.warn("", e);
            }
        }

        public void run() {
            log.warn("[trashman] started with interval=" + interval + ", max run time=" + TreeCommonParameters.trashMaxTime);
            while (running.get()) {
                try {
                    processTrash(false);
                    synchronized (this) {
                        wait(interval);
                    }
                } catch (InterruptedException ex) {
                    log.warn("[trashman] exit on interrupt");
                    return;
                }
            }
            log.warn("[trashman] exit on terminate");
        }
    }

    @Override
    public String toString() {
        return "Tree@" + root;
    }

    static class ChangeLogWriter {

        private FileOutputStream fileLog;
        private BufferedOutputStream bufferedLog;
        private HotMap<DBKey, Integer> changeSet;
        private int changeSetMax;

        ChangeLogWriter(File newlog, int maxBuffer) throws FileNotFoundException {
            fileLog = new FileOutputStream(newlog, true);
            bufferedLog = new BufferedOutputStream(fileLog, 4096);
            changeSet = new HotMap<DBKey, Integer>(new HashMap());
            changeSetMax = maxBuffer;
        }

        void write(DBKey key, TreeNode value) {
            synchronized (this) {
                if (value == null) {
                    changeSet.put(key, -1);
                } else {
                    Integer i = changeSet.get(key);
                    if (i == null) {
                        changeSet.put(key, 1);
                    } else {
                        changeSet.put(key, i + 1);
                    }
                }
                if (changeSet.size() > changeSetMax) {
                    Entry<DBKey, Integer> e = changeSet.removeEldestEntry();
                    DBKey k = e.getKey();
                    Integer i = e.getValue();
                    try {
                        Bytes.writeInt(i, bufferedLog);
                        Bytes.writeBytes(k.toBytes(), bufferedLog);
                    } catch (Exception ex)  {
                        log.warn("", ex);
                    }
                }
            }
        }

        void close() {
            try {
                synchronized (fileLog) {
                    bufferedLog.flush();
                    fileLog.close();
                }
            } catch (Exception ex)  {
                log.warn("", ex);
            }
            fileLog = null;
        }
    }

    static class ChangeLogEntry implements Map.Entry<DBKey, TreeNode> {

        private DBKey key;
        private TreeNode node;

        ChangeLogEntry(DBKey key, TreeNode node) {
            this.key = key;
            this.node = node;
        }

        @Override
        public DBKey getKey() {
            return key;
        }

        @Override
        public TreeNode getValue() {
            return node;
        }

        @Override
        public TreeNode setValue(TreeNode value) {
            TreeNode old = node;
            node = value;
            return old;
        }
    }

    class ChangeLogIterator implements ClosableIterator<Map.Entry<DBKey, TreeNode>> {

        private FileInputStream in;
        private BufferedInputStream buf;
        private ChangeLogEntry next;

        ChangeLogIterator(File log) throws FileNotFoundException {
            this.in = new FileInputStream(log);
            this.buf = new BufferedInputStream(in);
        }

        void fetchNext() {
            try {
                int i = Bytes.readInt(buf);
                DBKey key = new DBKey(Bytes.readBytes(buf));
                next = i > 0 ? new ChangeLogEntry(key, sourceGet(key)) : null;
            } catch (Exception ex) {
                try {
                    in.close();
                } catch (Exception ce)  {
                    log.warn("", ce);
                }
                in = null;
                if (!(ex instanceof EOFException)) {
                    log.trace("", ex);
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            if (next == null && in != null) {
                fetchNext();
            }
            return next != null;
        }

        @Override
        public ChangeLogEntry next() {
            if (hasNext()) {
                ChangeLogEntry ret = next;
                next = null;
                return ret;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
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
        TreeNode rootNode = getRootNode();
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

}

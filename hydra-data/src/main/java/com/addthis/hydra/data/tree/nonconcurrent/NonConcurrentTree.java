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
package com.addthis.hydra.data.tree.nonconcurrent;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Meter;
import com.addthis.hydra.data.tree.CacheKey;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeActor;
import com.addthis.hydra.data.tree.DataTreeNodeInitializer;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeCommonParameters;
import com.addthis.hydra.data.tree.TreeDataParent;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.concurrent.ConcurrentTreeNode;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPage;
import com.addthis.hydra.store.util.MeterFileLogger;
import com.addthis.hydra.store.util.MeterFileLogger.MeterDataSource;
import com.addthis.hydra.store.util.Raw;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * This is a non-concurrent version of the {@link DataTree}.  The idea here is
 * that in many cases a single thread that does not require concurrency constructs
 * may out preform concurrent implementations and it should be easier to reason about
 * the behavior of the class.
 * <p>
 * This class is explicitly not thread safe.  There is no protection against concurrent
 * modification of tree nodes or conflicting writes to the backing cache.
 * <p>
 * There are certainly more optimizations that can be made to differentiate this class
 * from its Concurrent cousin {@link com.addthis.hydra.data.tree.concurrent.ConcurrentTree}
 * but Rome wasn't built in a day.
 */
public final class NonConcurrentTree implements DataTree, MeterDataSource {
    static final Logger log = LoggerFactory.getLogger(NonConcurrentTree.class);

    @Override
    public Map<String, Long> getIntervalData() {
        return new HashMap<>();
    }

    public static enum METERTREE {
        NODE_PUT, NODE_CREATE, NODE_DELETE, SOURCE_MISS
    }

    private final File root;
    final IPageDB<DBKey, NonConcurrentTreeNode> source;
    private final NonConcurrentTreeNode treeRootNode;
    private final AtomicLong nextDBID;
    final AtomicBoolean closed = new AtomicBoolean(false);
    private final Meter<METERTREE> meter;
    private final MeterFileLogger logger;

    @GuardedBy("treeTrashNode")
    private IPageDB.Range<DBKey, ConcurrentTreeNode> trashIterator;


    public NonConcurrentTree(File root, int maxCacheSize,
                             int maxPageSize, PageFactory factory) throws Exception {
        LessFiles.initDirectory(root);
        this.root = root;
        long start = System.currentTimeMillis();

        // setup metering
        meter = new Meter<>(METERTREE.values());
        for (METERTREE m : METERTREE.values()) {
            meter.addCountMetric(m, m.toString());
        }

        // create meter logging thread
        if (TreeCommonParameters.meterLogging > 0) {
            logger = new MeterFileLogger(this, root, "tree-metrics",
                    TreeCommonParameters.meterLogging, TreeCommonParameters.meterLogLines);
        } else {
            logger = null;
        }
        source = new PageDB.Builder<>(root, NonConcurrentTreeNode.class, maxPageSize, maxCacheSize)
                .pageFactory(factory).build();
        source.setCacheMem(TreeCommonParameters.maxCacheMem);
        source.setPageMem(TreeCommonParameters.maxPageMem);
        source.setMemSampleInterval(TreeCommonParameters.memSample);

        // get stored next db id
        File idFile = new File(root, "nextID");
        if (idFile.exists() && idFile.isFile() && idFile.length() > 0) {
            nextDBID = new AtomicLong(Long.parseLong(LessBytes.toString(LessFiles.read(idFile))));
        } else {
            nextDBID = new AtomicLong(1);
        }

        // get tree root
        NonConcurrentTreeNode dummyRoot = NonConcurrentTreeNode.getTreeRoot(this);
        treeRootNode = dummyRoot.getOrCreateEditableNode("root");

        long openTime = System.currentTimeMillis() - start;
        log.info("dir={} root={} nextdb={} openms={}",
                root, treeRootNode, nextDBID, openTime);
    }

    public NonConcurrentTree(File root) throws Exception {
        this(root, TreeCommonParameters.maxCacheSize, TreeCommonParameters.maxPageSize,
                NonConcurrentPage.NonConcurrentPageFactory.singleton);
    }

    public void meter(METERTREE meterval) {
        meter.inc(meterval);
    }

    /**
     * This method is only for testing purposes.
     * It has a built in safeguard but nonetheless
     * it should not be invoked for other purposes.
     */
    @VisibleForTesting
    boolean setNextNodeDB(long id) {
        while (true) {
            long current = nextDBID.get();
            if (current > id) {
                return false;
            } else if (nextDBID.compareAndSet(current, id)) {
                return true;
            }
        }
    }

    long getNextNodeDB() {
        return nextDBID.incrementAndGet();
    }

    public NonConcurrentTreeNode getNode(final NonConcurrentTreeNode parent, final String child, final boolean lease) {
        long nodedb = parent.nodeDB();
        if (nodedb <= 0) {
            log.trace("[node.get] {} --> {} NOMAP --> null", parent, child);
            return null;
        }
        CacheKey key = new CacheKey(nodedb, child);
        DBKey dbkey = key.dbkey();
        NonConcurrentTreeNode node = source.get(dbkey);
        if (node == null) {
            meter.inc(METERTREE.SOURCE_MISS);
            return null; // (3)
        }
        node.initNode(this, dbkey, key.name);
        return node;
    }


    public NonConcurrentTreeNode getOrCreateNode(final NonConcurrentTreeNode parent, final String child,
                                                 final DataTreeNodeInitializer creator) {
        parent.requireNodeDB();
        CacheKey key = new CacheKey(parent.nodeDB(), child);

        DBKey dbkey = key.dbkey();
        NonConcurrentTreeNode node = source.get(dbkey);

        if (node != null) {
            node.initNode(this, dbkey, key.name);
            return node;
        } else { // create a new node
            NonConcurrentTreeNode newNode = new NonConcurrentTreeNode();
            newNode.init(this, dbkey, key.name);
            if (creator != null) {
                creator.onNewNode(newNode);
            }
            source.put(dbkey, newNode);
            parent.updateNodeCount(1);
            return newNode;
        }
    }

    @Override
    public void foregroundNodeDeletion(BooleanSupplier terminationCondition) {
        // do nothing
    }

    @Override
    public int getCacheSize() {
        return 0;
    }

    @Override
    public double getCacheHitRate() {
        return 0;
    }

    boolean deleteNode(final NonConcurrentTreeNode parent, final String child) {
        log.trace("[node.delete] {} --> {}", parent, child);
        long nodedb = parent.nodeDB();
        if (nodedb <= 0) {
            log.debug("parent has no children on delete : {} --> {}", parent, child);
            return false;
        }
        CacheKey key = new CacheKey(nodedb, child);
        NonConcurrentTreeNode node = source.remove(key.dbkey());
        if (node != null) {
            parent.updateNodeCount(-1);
            if (node.hasNodes() && !node.isAlias()) {
                deleteSubTree(node, 0);
            }
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    IPageDB.Range<DBKey, NonConcurrentTreeNode> fetchNodeRange(long db) {
        return source.range(new DBKey(db), new DBKey(db + 1));
    }

    @SuppressWarnings({"unchecked", "unused"})
    private IPageDB.Range<DBKey, NonConcurrentTreeNode> fetchNodeRange(long db, String from) {
        return source.range(new DBKey(db, Raw.get(from)), new DBKey(db + 1));
    }

    @SuppressWarnings("unchecked")
    IPageDB.Range<DBKey, NonConcurrentTreeNode> fetchNodeRange(long db, String from, String to) {
        return source.range(new DBKey(db, Raw.get(from)),
                to == null ? new DBKey(db + 1, (Raw) null) : new DBKey(db, Raw.get(to)));
    }

    @Override
    public NonConcurrentTreeNode getRootNode() {
        return treeRootNode;
    }

    @Override
    public long getDBCount() {
        return nextDBID.get();
    }

    /**
     * Close the tree.
     *
     * @param cleanLog  if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     */
    @Override
    public void close(boolean cleanLog, CloseOperation operation) throws IOException {
        if (!closed.compareAndSet(false, true)) {
            log.trace("already closed");
            return;
        }
        log.debug("closing {}", this);
        sync();
        if (source != null) {
            int status = source.close(cleanLog, operation);
            if (status != 0) {
                throw new RuntimeException("page db close returned a non-zero exit code : " + status);
            }
        }
        if (logger != null) {
            logger.terminate();
        }
    }

    @Override
    public void sync() throws IOException {
        source.put(treeRootNode.getDbkey(), treeRootNode);
    }


    @Override
    public void close() throws IOException {
        close(false, CloseOperation.NONE);
    }

    @Override
    public String toString() {
        return "Tree@" + root;
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
        NonConcurrentTreeNode rootNode = getRootNode();
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


    /**
     * Iteratively delete all the children of the input node.
     * Use a non-negative value for the counter parameter to
     * tally the nodes that have been deleted. Use a negative
     * value to disable logging of the number of deleted nodes.
     *
     * @param rootNode root of the subtree to delete
     * @param counter  if non-negative then tally the nodes that have been deleted
     */
    long deleteSubTree(NonConcurrentTreeNode rootNode,
                       long counter) {
        Stack<DeletePopper> stack = new Stack<>();
        stack.push(new DeletePopper(rootNode, counter));

        while (!stack.isEmpty()) {
            DeletePopper popper = stack.pop();

            long nodeDB = popper.node.nodeDB();
            IPageDB.Range<DBKey, NonConcurrentTreeNode> range = fetchNodeRange(nodeDB);
            DBKey endRange;
            try {
                while (range.hasNext()) {
                    if ((++counter % 1000) == 0) {
                        log.info("Deleted {} nodes from the tree.", counter);
                    }
                    Map.Entry<DBKey, NonConcurrentTreeNode> entry = range.next();
                    NonConcurrentTreeNode next = entry.getValue();

                    if (next.hasNodes() && !next.isAlias()) {
                        stack.push(new DeletePopper(next, counter));
                    }
                }
                endRange = new DBKey(nodeDB + 1);
            } finally {
                range.close();
            }
            source.remove(new DBKey(nodeDB), endRange);
        }
        return counter;
    }

    private class DeletePopper {
        private final NonConcurrentTreeNode node;
        private final long counter;

        public DeletePopper(NonConcurrentTreeNode node, long counter) {
            this.node = node;
            this.counter = counter;
        }
    }
}

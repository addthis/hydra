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

import java.io.File;
import java.io.IOException;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB.Range;
import com.addthis.hydra.store.db.ReadPageDB;
import com.addthis.hydra.store.kv.ReadExternalPagedStore;
import com.addthis.hydra.store.util.Raw;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * <p/>
 * Read-only Tree (for querying)
 * <p/>
 * Has a root file where it locates a database to use as its backing source.
 * Keeps a cache of tree nodes to save on loading frequently used nodes.
 * <p/>
 * MUST BE CLOSED. It is not okay to simply dereference. (due to the bdb instance it uses).
 */
public final class ReadTree implements DataTree {

    private static final Logger log = LoggerFactory.getLogger(ReadTree.class);

    // max number of nodes allowed to reside in memory. Zero means unlimited (not recommended)
    private static final int nodeCacheSize = Parameter.intValue("hydra.tree.cache.nodeCacheSize", 250);

    /*  total node weight allowed to reside in memory. Correlates with byte representation of node, but is only a
        scaled heuristic of memory usage. The average, empty node should be about 24 weight. At that amount, its
        in-memory representation is dominated by overhead stuff and its string name. By default, we will leave the
        comparison of hydrated lean/fat nodes up to the weigher and conservatively allocate for all lean nodes.

        If set to zero, then nodeCacheSize will be used and nodes will be unweighted */
    private static final int nodeCacheWeight = Parameter.intValue("hydra.tree.cache.nodeCacheWeight", nodeCacheSize * 24);


    /*  max number of pages allowed to reside in memory. When pageCacheWeight is default, this value
        is used to calculate pageCacheWeight. With a default pageCacheWeight or with pageCacheWeight == 0,
        a 0 for this value means unlimited caching (not recommended), and negative values will not end well.*/
    private static final int pageCacheSize = Parameter.intValue("hydra.tree.cache.pageCacheSize", 500);

    /* maxBytes is similar to nodeCacheWeight. The canonical 'lean page' is estimated as follows:
            50 page entries * (10 byte key + 24 byte payload) + 24 metadata bytes

        The default value uses that estimation * pageCacheSize.
        A value of zero disables page weights and uses page count only. */
    private static final int pageCacheWeight = Parameter.intValue("hydra.tree.cache.pageCacheWeight", pageCacheSize * 1724);

    private final File root;
    private final ReadPageDB<ReadTreeNode> source;
    private final ReadTreeNode treeRootNode;
    private final LoadingCache<CacheKey, ReadTreeNode> loadingNodeCache;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean metrics;

    public ReadTree(File root) throws Exception {
        this(root, false);
    }

    public ReadTree(File root, boolean metrics) throws Exception {
        this.metrics = metrics;

        if (!root.isDirectory()) {
            throw new IOException("Unable to open root directory '" + root + "'");
        }

        this.root = root;
        source = initSource();
        try {
            if (nodeCacheWeight != 0) {
                // limit by weight
                loadingNodeCache = CacheBuilder.newBuilder()
                        .maximumWeight(nodeCacheWeight)
                        .weigher(new Weigher<CacheKey, ReadTreeNode>() {
                            @Override
                            public int weigh(CacheKey key, ReadTreeNode value) {
                            /* A lean node goes from 24 to 24 + its string name and + cacheKey. the 24 becomes a small percentage.

                                Dangerous, fat nodes typically have lots of serialized strings in their value payload. The inflation
                                ratio there is actually probably less than for lean nodes since the various pointers for the string
                                objects may not be nearly as large as the strings themselves. Therefore, holding them to the lean
                                node's expansion standard is probably conservative enough. */
                                return value.getWeight();
                            }
                        })
                        .build(
                                new CacheLoader<CacheKey, ReadTreeNode>() {
                                    public ReadTreeNode load(CacheKey key) throws Exception {
                                        ReadTreeNode node = sourceGet(key.dbkey());
                                        if (node != null) {
                                            node.init(ReadTree.this, key.name);
                                            return node;
                                        } else {
                                            throw new ExecutionException("Source did not have node", new NullPointerException());
                                        }
                                    }
                                });
            } else {
                // Limit by the number of nodes
                loadingNodeCache = CacheBuilder.newBuilder()
                        .maximumSize(nodeCacheSize)
                        .build(
                                new CacheLoader<CacheKey, ReadTreeNode>() {
                                    public ReadTreeNode load(CacheKey key) throws Exception {
                                        ReadTreeNode node = sourceGet(key.dbkey());
                                        if (node != null) {
                                            node.init(ReadTree.this, key.name);
                                            return node;
                                        } else {
                                            throw new ExecutionException("Source did not have node", new NullPointerException());
                                        }
                                    }
                                });
            }
            treeRootNode = getNode(1, "root");
            if (treeRootNode == null) {
                throw new RuntimeException("missing root in readonly tree");
            }
        } catch (Exception e) {
            source.close();
            throw e;
        }
    }

    /**
     * Creates the ReadPageDB source object and also emits some timing metrics for that operation.
     *
     * @return the source - make sure to close it eventually
     * @throws Exception
     */
    private ReadPageDB<ReadTreeNode> initSource() throws Exception {
        long start = System.currentTimeMillis();

        //open page db (opens byte store and bdb as well)
        ReadPageDB<ReadTreeNode> source = new ReadPageDB<>(root, ReadTreeNode.class,
                pageCacheSize, pageCacheWeight, metrics);

        long openTime = System.currentTimeMillis() - start;
        log.warn("dir=" + root + " openms=" + openTime);
        return source;
    }

    /**
     * Preloads the cache keys given to it under the presumption that they will be asked for
     * again in the near future. Doesn't include the eviction hinting status of them, but could
     * be better than nothing.
     */
    public void warmCacheFrom(Iterable<CacheKey> keys) {
        try {
            loadingNodeCache.getAll(keys);
        } catch (ExecutionException e) {
            // Some of the nodes failed to load -- expected if they were deleted/pruned when job last ran
        }
    }

    /**
     * Returns an iterable of cache keys representing a weakly consistent view of the cache. Mostly to
     * be used for warming other caches but could also be helpful for metrics or debugging.
     *
     * @return the iterable object
     */
    public Iterable<CacheKey> getCacheIterable() {
        return loadingNodeCache.asMap().keySet();
    }

    /**
     * Method that wraps access to the loading node cache. Returns a node from the tree with the given
     * parent id and the given name.
     *
     * @param parentID  - integer parent id of the node's parent
     * @param childName - name of the node desired
     * @return the node desired
     */
    protected ReadTreeNode getNode(int parentID, final String childName) {
        try {
            CacheKey key = new CacheKey(parentID, childName);
            ReadTreeNode node = loadingNodeCache.get(key);
            if (log.isTraceEnabled()) {
                log.trace("[node.get] " + parentID + " --> " + childName + " --> " + node);
            }
            return node;
        } catch (ExecutionException e) { //Source does not have node
            return null;
        } catch (Exception e) { //Unexpected runtime error from source
            log.error("", e);
            return null;
        }
    }

    /**
     * Returns a DataTreeNode given a ReadTreeNode parent node and the name of the child. Just
     * extracts the integer parent id from the parent node and then calls getNode(int, string).
     *
     * @param parent - parent node
     * @param child  - name of child
     * @return child
     */
    protected DataTreeNode getNode(final ReadTreeNode parent, final String child) {
        Integer nodedb = parent.nodeDB();
        if (nodedb == null) {
            if (log.isTraceEnabled()) {
                log.trace("[node.get] " + parent + " --> " + child + " NOMAP --> null");
            }
            return null;
        }
        return getNode(nodedb, child);
    }

    /**
     * Gets a node from the backing store (eg ReadPageDB) for a given DBKey. DBKeys are generally
     * obtained from CacheKeys. This method should probably only be called from the loading node cache's
     * load method.
     *
     * @param key - db key for desired node
     * @return desired node
     */
    protected ReadTreeNode sourceGet(final DBKey key) {
        ReadTreeNode node = source.get(key);
        if (log.isTraceEnabled()) {
            log.trace("[source.get] " + key + " --> " + node);
        }
        return node;
    }

    @SuppressWarnings("unchecked")
    protected Range<DBKey, ReadTreeNode> fetchNodeRange(int db) {
        return source.range(new DBKey(db), new DBKey(db + 1));
    }

    protected Range<DBKey, ReadTreeNode> fetchNodeRange(int db, int sampleRate) {
        return source.range(new DBKey(db), new DBKey(db + 1), sampleRate);
    }

    @SuppressWarnings("unchecked")
    protected Range<DBKey, ReadTreeNode> fetchNodeRange(int db, String from, String to) {
        return source.range(new DBKey(db, Raw.get(from)), to == null ? new DBKey(db+1, (Raw)null) : new DBKey(db, Raw.get(to)));
    }

    public ReadTreeNode getRootNode() {
        return treeRootNode;
    }

    @Override
    public void sync() {

    }

    /**
     * Close the source.
     *
     * @param cleanLog unused in the ReadTree implementation.
     * @param operation unused in the ReadTree implementation.
     **/
    @Override
    public void close(boolean cleanLog, CloseOperation operation) {
        close();
    }

    /**
     * Must be called to close the source. The source generally considers being closed
     * to be pretty important.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.trace("already closed");
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("closing " + this);
        }
        try {//source is final in read-tree
            source.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    @Override
    public int getDBCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCacheSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getCacheHitRate() {
        throw new UnsupportedOperationException();
    }

    /**
     * The object that acts as the hash-key for the loading node cache. Also importantly provides the DBKey
     * used to pull nodes from the underlying data store. This class is static so that it will not hold a
     * reference to the enclosing read tree. This is important so that CacheKeys can be passed between Trees
     * without preventing old Trees from being garbage collected (it also saves a small amount of heap space).
     */
    protected static class CacheKey {

        private final int hc;
        private final int parentID;
        private final String name;

        protected CacheKey(int parentID, String name) {
            this.hc = Objects.hash(parentID, name);
            this.parentID = parentID;
            this.name = name;
        }

        protected DBKey dbkey() {
            return new DBKey(parentID, Raw.get(name));
        }

        @Override
        public boolean equals(Object key) {
            CacheKey ck = (CacheKey) key;
            return ck.parentID == parentID && ck.name.equals(name);
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

    /**
     * These methods implement the node interface. Mostly they just wrap the root node's methods of the
     * same name, although if they are a write-based method we just go ahead and throw the unsupported
     * operation exception (which the root node would probably throw anyway).
     */

    @Override
    public DataTreeNode getNode(String name) {
        return getRootNode().getNode(name);
    }

    @Override
    public DataTreeNode getLeasedNode(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTreeNode getOrCreateNode(String name, DataTreeNodeInitializer init) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteNode(String node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator() {
        ReadTreeNode rootNode = getRootNode();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementCounter(long val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCounter(long val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean aliasTo(DataTreeNode target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lease() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTreeNodeActor getData(String key) {
        return getRootNode().getData(key);
    }

    @Override
    public Map<String, TreeNodeData> getDataMap() {
        return getRootNode().getDataMap();
    }

    public ReadExternalPagedStore<DBKey, ReadTreeNode> getReadEps() {
        return source.getReadEps();
    }

    /**
     * For testing purposes only.
     */
    void testIntegrity() {
        ReadExternalPagedStore store = source.getReadEps();
        store.testIntegrity();
    }

}

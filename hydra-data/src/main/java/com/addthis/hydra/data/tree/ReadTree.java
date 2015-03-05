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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.ReadPageDB;
import com.addthis.hydra.store.kv.ReadExternalPagedStore;
import com.addthis.hydra.store.util.Raw;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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

    /**
     * Represents missing nodes in the cache so that we don't have to do repeated look ups or deal with exceptions.
     * A weight of 8 was chosen as a rough estimate of the relative overhead of the cache entry for each key.
     */
    private static final ReadTreeNode MISSING = new ReadTreeNode("missing", 8);

    private final File root;
    private final ReadPageDB<ReadTreeNode> source;
    private final ReadTreeNode rootNode;
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
            CacheBuilder<? super CacheKey, ? super ReadTreeNode> cacheBuilder = CacheBuilder.newBuilder();
            if (nodeCacheWeight != 0) {
                // limit by weight
                cacheBuilder = cacheBuilder
                        .maximumWeight(nodeCacheWeight)
                        .weigher((key, value) -> {
                            /* A lean node goes from 24 to 24 + its string name and + cacheKey. the 24 becomes a
                            small percentage.

                            Dangerous, fat nodes typically have lots of serialized strings in their value payload.
                            The inflation ratio there is actually probably less than for lean nodes since the various
                            pointers for the string objects may not be nearly as large as the strings themselves.
                            Therefore, holding them to the lean node's expansion standard is probably conservative
                            enough. */
                            return value.getWeight();
                        });
            } else {
                // Limit by the number of nodes
                cacheBuilder = cacheBuilder.maximumSize(nodeCacheSize);
            }
            loadingNodeCache = cacheBuilder.build(
                    new CacheLoader<CacheKey, ReadTreeNode>() {
                        @Override public ReadTreeNode load(CacheKey key) throws Exception {
                            ReadTreeNode node = sourceGet(key.dbkey());
                            if (node != null) {
                                node.init(ReadTree.this, key.name);
                                return node;
                            } else {
                                return MISSING;
                            }
                        }
                    });
            rootNode = getNode(1, "root");
            if (rootNode == null) {
                throw new IllegalStateException("missing root in readonly tree");
            }
        } catch (Exception e) {
            source.close();
            throw e;
        }
    }

    /**
     * Creates the ReadPageDB source object and also emits some timing metrics for that operation. The returned source
     * MUST be closed when no longer needed.
     */
    private ReadPageDB<ReadTreeNode> initSource() throws Exception {
        long start = System.currentTimeMillis();

        //open page db (opens byte store and bdb as well)
        ReadPageDB<ReadTreeNode> source = new ReadPageDB<>(root, ReadTreeNode.class,
                pageCacheSize, pageCacheWeight, metrics);

        long openTime = System.currentTimeMillis() - start;
        log.info("dir={} openms={}", root, openTime);
        return source;
    }

    /**
     * Preloads the cache keys given to it under the presumption that they may be asked for again in the near future.
     * Doesn't include the eviction hinting status of them, but better than nothing.
     */
    public void warmCacheFrom(Iterable<CacheKey> keys) {
        try {
            loadingNodeCache.getAll(keys);
        } catch (Exception e) {
            log.error("Unexpected error warming cache for {} from {}", this, keys, e);
        }
    }

    /**
     * Returns an iterable of cache keys representing a weakly consistent view of the cache. Mostly to
     * be used for warming other caches but could also be helpful for metrics or debugging.
     */
    public Iterable<CacheKey> getCacheIterable() {
        return loadingNodeCache.asMap().keySet();
    }

    /**
     * Returns a node from the tree with the given parent id and the given name. This mostly just wraps access to
     * the loading node cache.
     */
    @Nullable private ReadTreeNode getNode(long parentID, final String childName) {
        try {
            CacheKey key = new CacheKey(parentID, childName);
            ReadTreeNode node = loadingNodeCache.get(key);
            if (node == MISSING) {
                log.trace("[node.get] {} --> {} --> MISSING", parentID, childName);
                return null;
            } else {
                log.trace("[node.get] {} --> {} --> {}", parentID, childName, node);
                return node;
            }
        } catch (Exception e) {
            log.error("Unexpected error loading node ({}:{}) from tree ({})", parentID, childName, this, e);
            return null;
        }
    }

    /**
     * Returns a DataTreeNode given a ReadTreeNode parent node and the name of the child. Just
     * extracts the integer parent id from the parent node and then calls getNode(int, string).
     */
    @Nullable DataTreeNode getNode(final ReadTreeNode parent, final String child) {
        long nodedb = parent.nodeDB();
        if (nodedb <= 0) {
            log.trace("[node.get] {} --> {} NOMAP --> null", parent, child);
            return null;
        }
        return getNode(nodedb, child);
    }

    /**
     * Gets a node from the backing store (eg ReadPageDB) for a given DBKey. DBKeys are generally obtained from
     * CacheKeys. This method should probably only be called from the loading node cache's load method.
     */
    private ReadTreeNode sourceGet(final DBKey key) {
        ReadTreeNode node = source.get(key);
        log.trace("[source.get] {} --> {}", key, node);
        return node;
    }

    @Override @Nonnull public ReadTreeNode getRootNode() {
        return rootNode;
    }

    /** Must be called to close the source. The source generally considers being closed to be pretty important. */
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.trace("already closed");
            return;
        }
        log.debug("closing {}", this);
        try {
            source.close();
        } catch (Exception ex)  {
            log.error("While closing source:", ex);
        }
    }

    @Override public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                                             .add("nodeCacheSize", nodeCacheSize)
                                             .add("nodeCacheWeight", nodeCacheWeight)
                                             .add("pageCacheSize", pageCacheSize)
                                             .add("pageCacheWeight", pageCacheWeight)
                                             .add("root", root)
                                             .add("source", source)
                                             .add("rootNode", rootNode)
                                             .add("loadingNodeCache", loadingNodeCache)
                                             .add("closed", closed)
                                             .add("metrics", metrics)
                                             .toString();
    }

    IPageDB.Range<DBKey, ReadTreeNode> fetchNodeRange(long db) {
        return source.range(new DBKey(db), new DBKey(db + 1));
    }

    IPageDB.Range<DBKey, ReadTreeNode> fetchNodeRange(long db, int sampleRate) {
        return source.range(new DBKey(db), new DBKey(db + 1), sampleRate);
    }

    IPageDB.Range<DBKey, ReadTreeNode> fetchNodeRange(long db, String from, String to) {
        if (to == null) {
            return source.range(new DBKey(db, Raw.get(from)), new DBKey(db + 1, (Raw) null));
        } else {
            return source.range(new DBKey(db, Raw.get(from)), new DBKey(db, Raw.get(to)));
        }
    }

    /**
     * The object that acts as the hash-key for the loading node cache. Also importantly provides the DBKey
     * used to pull nodes from the underlying data store. This class is static so that it will not hold a
     * reference to the enclosing read tree. This is important so that CacheKeys can be passed between Trees
     * without preventing old Trees from being garbage collected (it also saves a small amount of heap space).
     */
    protected static class CacheKey {

        private final int hc;
        private final long parentID;
        private final String name;

        protected CacheKey(long parentID, String name) {
            this.hc = Objects.hash(parentID, name);
            this.parentID = parentID;
            this.name = name;
        }

        protected DBKey dbkey() {
            return new DBKey(parentID, Raw.get(name));
        }

        @Override public boolean equals(Object obj) {
            if (obj instanceof CacheKey) {
                CacheKey ck = (CacheKey) obj;
                return (ck.parentID == parentID) && ck.name.equals(name);
            } else {
                return false;
            }
        }

        @Override public int hashCode() {
            return hc;
        }
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

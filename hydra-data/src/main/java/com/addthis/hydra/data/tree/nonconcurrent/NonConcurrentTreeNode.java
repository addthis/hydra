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
import com.addthis.basis.util.MemoryCounter.Mem;
import com.addthis.hydra.data.tree.AbstractTreeNode;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeActor;
import com.addthis.hydra.data.tree.DataTreeNodeInitializer;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeDataParent;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeDataDeferredOperation;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB.Range;

import java.util.*;
import java.util.Map.Entry;


/**
 * A non-concurrent implementation of {@link AbstractTreeNode}.  This
 * instance is not thread safe (as the name implies) and is designed
 * to work efficiently in a single threaded environment
 */
public class NonConcurrentTreeNode extends AbstractTreeNode {

    private String name;
    private DBKey dbkey;

    public static NonConcurrentTreeNode getTreeRoot(NonConcurrentTree tree) {
        NonConcurrentTreeNode node = new NonConcurrentTreeNode();
        node.tree = tree;
        node.nodedb = 1L;
        return node;
    }

    /**
     * required for Codable. must be followed by an init() call.
     */
    public NonConcurrentTreeNode() {
    }

    protected void initNode(NonConcurrentTree tree, DBKey key, String name) {
        this.tree = tree;
        this.dbkey = key;
        this.name = name;
    }

    protected void init(NonConcurrentTree tree, DBKey key, String name) {
        this.tree = tree;
        this.dbkey = key;
        this.name = name;
    }

    @Mem(estimate = false, size = 64)
    private NonConcurrentTree tree;

    public String toString() {
        return "TN[k=" + dbkey + ",db=" + nodedb + ",n#=" + nodes + ",h#=" + hits +
                ",nm=" + name + ",bi=" + bits + "]";
    }

    @Override
    public String getName() {
        return name;
    }


    protected int updateNodeCount(int delta) {
        nodes += delta;
        return nodes;
    }

    /**
     * double-checked locking idiom to avoid unnecessary synchronization.
     */
    protected void requireNodeDB() {
        if (!hasNodes()) {
            nodedb = tree.getNextNodeDB();
        }
    }


    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator() {
        return !hasNodes() ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb));
    }

    @Override
    public NonConcurrentTree getTreeRoot() {
        return tree;
    }

    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String prefix) {
        if (prefix != null && prefix.length() > 0) {
            /**
             * the reason this behaves as a prefix as opposed to a "from" is the way
             * the "to" endpoint is calculated.  the last byte of the prefix is incremented
             * by a value of one and used as "to".  this is the effect of excluding all
             * other potential matches with a lexicographic value greater than prefix.
             */
            StringBuilder sb = new StringBuilder(prefix.substring(0, prefix.length() - 1));
            sb.append((char) (prefix.charAt(prefix.length() - 1) + 1));
            return getNodeIterator(prefix, sb.toString());
        } else {
            return new Iter(null);
        }
    }

    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String from, String to) {
        if (!hasNodes()) {
            return new Iter(null);
        }
        return new Iter(tree.fetchNodeRange(nodedb, from, to));
    }

    @Override
    public NonConcurrentTreeNode getNode(String name) {
        return tree.getNode(this, name, false);
    }


    @Override
    public ClosableIterator<DataTreeNode> getIterator() {
        return getNodeIterator();
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String prefix) {
        return getNodeIterator(prefix);
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String from, String to) {
        return getNodeIterator(from, to);
    }

    @Override
    public NonConcurrentTreeNode getLeasedNode(String name) {
        return tree.getNode(this, name, true);
    }

    public NonConcurrentTreeNode getOrCreateEditableNode(String name) {
        return getOrCreateEditableNode(name, null);
    }

    public NonConcurrentTreeNode getOrCreateEditableNode(String name, DataTreeNodeInitializer creator) {
        return tree.getOrCreateNode(this, name, creator);
    }

    @Override
    public boolean deleteNode(String name) {
        return tree.deleteNode(this, name);
    }

    /**
     * link this node (aliasing) to another node in the tree. they will share
     * children, but not meta-data. should only be called from within a
     * TreeNodeInitializer passed to getOrCreateEditableNode.
     */
    @Override
    public boolean aliasTo(DataTreeNode node) {
        if (node.getClass() != NonConcurrentTreeNode.class) {
            return false;
        }
        if (hasNodes()) {
            return false;
        }
        ((NonConcurrentTreeNode) node).requireNodeDB();
        nodedb = ((NonConcurrentTreeNode) node).nodedb;
        markAlias();
        return true;
    }

    /**
     * TODO: warning. if you annotate a path with data then have another path
     * that intersects that node in the tree with some other data, the first one
     * wins and the new data will not be added. further, every time the
     * annotated node is crossed, the attached data will be updated if that path
     * declares annotated data.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path) {
        HashMap<String, TreeDataParameters> dataconf = path.dataConfig();
        if (path.assignHits()) {
            hits = state.getAssignmentValue();
        } else if (path.countHits()) {
            hits += state.getCountValue();
        }
        if (dataconf != null) {
            if (data == null) {
                data = new HashMap<>(dataconf.size());
            }
            for (Entry<String, TreeDataParameters> el : dataconf.entrySet()) {
                TreeNodeData tnd = data.get(el.getKey());
                if (tnd == null) {
                    tnd = el.getValue().newInstance(this);
                    data.put(el.getKey(), tnd);
                }
                if (tnd.updateChildData(state, this, el.getValue())) {
                }
            }
        }
    }

    /**
     * @return true if data was changed
     */
    @Override
    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew) {
        List<TreeNodeDataDeferredOperation> deferredOps = null;
        if (child != null && data != null) {
            deferredOps = new ArrayList<>(1);
            for (TreeNodeData<?> tnd : data.values()) {
                if (isnew) {
                    tnd.updateParentNewChild(state, this, child, deferredOps);
                }
                tnd.updateParentData(state, this, child, deferredOps);
            }
        }
        if (deferredOps != null) {
            for (TreeNodeDataDeferredOperation currentOp : deferredOps) {
                currentOp.run();
            }
        }
    }

    @Override
    public DataTreeNodeActor getData(String key) {
        return data != null ? data.get(key) : null;
    }

    @Override
    public void postDecode() {
        super.postDecode();
    }

    @Override
    public Iterator<DataTreeNode> iterator() {
        return null;
    }

    private final class Iter implements ClosableIterator<DataTreeNode> {

        private Range<DBKey, NonConcurrentTreeNode> range;
        private NonConcurrentTreeNode next;

        private Iter(Range<DBKey, NonConcurrentTreeNode> range) {
            this.range = range;
            fetchNext();
        }

        public String toString() {
            return "Iter(" + range + "," + next + ")";
        }

        void fetchNext() {
            if (range != null) {
                next = null;
                while (range.hasNext()) {
                    Entry<DBKey, NonConcurrentTreeNode> tne = range.next();
                    next = tree.getNode(NonConcurrentTreeNode.this, tne.getKey().rawKey().toString(), false);
                    if (next != null) {
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public NonConcurrentTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            NonConcurrentTreeNode ret = next;
            fetchNext();
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            if (range != null) {
                range.close();
                range = null;
            }
        }
    }

    @Override
    public DataTreeNode getOrCreateNode(String name, DataTreeNodeInitializer init) {
        return getOrCreateEditableNode(name, init);
    }


    @Override
    public long getCounter() {
        return hits;
    }

    @Override
    public void incrementCounter() {
        hits++;
    }

    @Override
    public long incrementCounter(long val) {
        hits += val;
        return hits;
    }

    @Override
    public void setCounter(long val) {
        hits = val;
    }

    @Override
    public void release() {
        // do nothing
    }

    public long nodeDB() {
        return nodedb;
    }


    protected HashMap<String, TreeNodeData> createMap() {
        if (data == null) {
            data = new HashMap<>();
        }
        return data;
    }

    @Override
    public int getNodeCount() {
        return nodes;
    }

    final boolean isBitSet(int bitcheck) {
        return (bits & bitcheck) == bitcheck;
    }

    public boolean isAlias() {
        return isBitSet(ALIAS);
    }

    protected final void bitSet(int set) {
        bits |= set;
    }

    protected synchronized void markAlias() {
        bitSet(ALIAS);
    }

    public DBKey getDbkey() {
        return dbkey;
    }
}

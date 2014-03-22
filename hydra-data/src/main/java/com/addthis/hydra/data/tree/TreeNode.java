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

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.MemoryCounter.Mem;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB.Range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TreeNode extends AbstractTreeNode {

    public static final int DELETED = 1 << 0;
    public static final int ALIAS = 1 << 1;
    public static final int ZOMBIE = 1 << 2;

    public static TreeNode getTreeRoot(Tree tree) {
        TreeNode node = new TreeNode() {
            @Override
            public boolean isEditable() {
                return true;
            }
        };
        node.tree = tree;
        node.leases.incrementAndGet();
        node.nodedb = 1;
        return node;
    }

    /**
     * required for Codable. must be followed by an init() call.
     */
    public TreeNode() {
    }

    protected void init(Tree tree, DBKey key, String name) {
        this.tree = tree;
        this.dbkey = key;
        this.name = name;
    }

    @Mem(estimate = false, size = 64)
    private Tree tree;
    @Mem(estimate = false, size = 64)
    private AtomicInteger leases = new AtomicInteger(0);
    @Mem(estimate = false, size = 64)
    private AtomicBoolean changed = new AtomicBoolean(false);
    @Mem(estimate = false, size = 64)
    private AtomicInteger cleanQcounter = new AtomicInteger(0);
    @Mem(estimate = false, size = 64)
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    protected boolean decoded;
    protected String name;
    protected DBKey dbkey;

    public TreeNode getTempClone() {
        TreeNode tn = new TreeNode();
        tn.name = name;
        tn.hits = hits;
        tn.nodes = nodes;
        tn.nodedb = nodedb;
        tn.bits = bits;
        tn.data = data;
        tn.tree = tree;
        tn.dbkey = dbkey;
        return tn;
    }

    public String toString() {
        return "TN[k=" + dbkey + ",db=" + nodedb + ",n#=" + nodes + ",h#=" + hits + ",nm=" + name + ",le=" + leases + ",ch=" + changed + ",bi=" + bits + "]";
    }

    public String getName() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, TreeNodeData> getDataMap() {
        return data;
    }

    public boolean hasNodes() {
        return nodedb != null;
    }

    public int getLeaseCount() {
        return leases.get();
    }

    protected synchronized int updateNodeCount(int delta) {
        nodes += delta;
        changed.set(true);
        return nodes;
    }

    private void requireEditable() {
        if (!isEditable()) {
            throw new RuntimeException("fail editable requirement");
        }
    }

    public final boolean isBitSet(int bitcheck) {
        return (bits & bitcheck) == bitcheck;
    }

    public boolean isAlias() {
        return isBitSet(ALIAS);
    }

    public boolean isDeleted() {
        return isBitSet(DELETED);
    }

    public boolean isEditable() {
        return leases.get() > 0;
    }

    protected void markChanged() {
        requireEditable();
        changed.set(true);
    }

    protected synchronized void markDeleted() {
        bitSet(DELETED);
    }

    protected synchronized void markAlias() {
        bitSet(ALIAS);
    }

    protected boolean getAndClearChanged() {
        return changed.getAndSet(false);
    }

    public boolean getAndClearDecoded() {
        if (decoded) {
            decoded = false;
            return true;
        }
        return false;
    }

    private final void bitSet(int set) {
        bits |= set;
    }

    private final void bitUnset(int set) {
        bits &= (~set);
    }

    @Override
    public void lease() {
        leases.incrementAndGet();
    }

    @Override
    public void release() {
        requireEditable();
        if (leases.decrementAndGet() == 0 && !isDeleted()) {
            tree.addToCleanQueue(this);
        }
    }

    protected void cleanQInc() {
        cleanQcounter.incrementAndGet();
    }

    protected boolean cleanQDec() {
        return cleanQcounter.decrementAndGet() == 0;
    }

    protected void requireNodeDB() {
        synchronized (this) {
            if (nodedb == null) {
                nodedb = tree.getNextNodeDB();
            }
        }
    }

    protected Integer nodeDB() {
        return nodedb;
    }

    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator() {
        return nodedb == null || isDeleted() ? new Iter(null, false) : new Iter(tree.fetchNodeRange(nodedb), true);
    }

    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String prefix) {
        if (!isDeleted() && prefix != null && prefix.length() > 0) {
            StringBuilder sb = new StringBuilder(prefix.substring(0, prefix.length() - 1));
            sb.append((char) (prefix.charAt(prefix.length() - 1) + 1));
            return getNodeIterator(prefix, sb.toString());
        } else {
            return new Iter(null, false);
        }
    }

    /**
     * returns an iterator of read-only nodes
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String from, String to) {
        return nodedb == null || isDeleted() ? new Iter(null, false) : new Iter(tree.fetchNodeRange(nodedb, from, to), true);
    }

    public TreeNode getNode(String name) {
        return tree.getNode(this, name, false);
    }

    public TreeNode getLeasedNode(String name) {
        return tree.getNode(this, name, true);
    }

    public DataTreeNode getOrCreateEditableNode(String name) {
        return getOrCreateEditableNode(name, null);
    }

    public DataTreeNode getOrCreateEditableNode(String name, DataTreeNodeInitializer creator) {
        return tree.getOrCreateNode(this, name, creator);
    }

    public boolean deleteNode(String node) {
        return tree.deleteNode(this, node);
    }

    /**
     * link this node (aliasing) to another node in the tree. they will share
     * children, but not meta-data. should only be called from within a
     * TreeNodeInitializer passed to getOrCreateEditableNode.
     */
    @Override
    public boolean aliasTo(DataTreeNode node) {
        if (node.getClass() != TreeNode.class) {
            return false;
        }
        requireEditable();
        if (nodedb != null) {
            return false;
        }
        ((TreeNode) node).requireNodeDB();
        nodedb = ((TreeNode) node).nodedb;
        markAlias();
        return true;
    }

    HashMap<String, TreeNodeData> createMap() {
        if (data == null) {
            data = new HashMap<>();
        }
        return data;
    }

    /**
     * TODO: warning. if you annotate a path with data then have another path
     * that intersects that node in the tree with some other data, the first one
     * wins and the new data will not be added. further, every time the
     * annotated node is crossed, the attached data will be updated if that path
     * declares annotated data.
     */
    @SuppressWarnings("unchecked")
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path) {
        requireEditable();
        boolean updated = false;
        HashMap<String, TreeDataParameters> dataconf = path.dataConfig();
        lock.writeLock().lock();
        try {
            if (path.countHits()) {
                hits += state.getCountValue();
                updated = true;
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
                        updated = true;
                    }
                    if (tnd.updateChildData(state, this, el.getValue())) {
                        updated = true;
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (updated) {
            changed.set(true);
        }
    }

    /**
     * @return true if data was changed
     */
    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew) {
        requireEditable();
        List<TreeNodeDataDeferredOperation> deferredOps = null;
        lock.writeLock().lock();
        try {
            if (child != null && data != null) {
                deferredOps = new ArrayList<>(1);
                for (TreeNodeData<?> tnd : data.values()) {
                    if (isnew && tnd.updateParentNewChild(state, this, child, deferredOps)) {
                        changed.set(true);
                    }
                    if (tnd.updateParentData(state, this, child, deferredOps)) {
                        changed.set(true);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (deferredOps != null) {
            for (TreeNodeDataDeferredOperation currentOp : deferredOps) {
                currentOp.run();
            }
        }
    }

    // TODO concurrent broken -- data classes should be responsible for their
    // own get/update sync
    public DataTreeNodeActor getData(String key) {
        lock.readLock().lock();
        try {
            return data != null ? data.get(key) : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    // TODO concurrent broken -- data classes should be responsible for their
    // own get/update sync
    public Collection<String> getDataFields() {
        lock.readLock().lock();
        try {
            if (data == null || data.size() == 0) {
                return null;
            }
            return data.keySet();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int getNodeCount() {
        return nodes;
    }

    @Override
    public void postDecode() {
        decoded = true;
        if (data != null) {
            for (TreeNodeData actor : data.values()) {
                actor.setBoundNode(this);
            }
        }
    }

    @Override
    public void preEncode() {
    }

    /**
     * TODO warning: not thread safe. sync around next(), hasNext() when
     * concurrency is required.
     */
    private final class Iter implements ClosableIterator<DataTreeNode> {

        private Range<DBKey, TreeNode> range;
        private TreeNode next;
        private boolean filterDeleted;

        private Iter(Range<DBKey, TreeNode> range, boolean filterDeleted) {
            this.range = range;
            this.filterDeleted = filterDeleted;
            fetchNext();
        }

        public String toString() {
            return "Iter(" + range + "," + next + ")";
        }

        void fetchNext() {
            if (range != null) {
                next = null;
                while (range.hasNext()) {
                    Entry<DBKey, TreeNode> tne = range.next();
                    next = tree.getNode(TreeNode.this, tne.getKey().rawKey().toString(), false);
                    if (next != null) {
                        if (filterDeleted && next.isDeleted()) {
                            next = null;
                            continue;
                        }
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
        public TreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            TreeNode ret = next;
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
    public boolean encodeLock() {
        lock.readLock().lock();
        return true;
    }

    @Override
    public void encodeUnlock() {
        lock.readLock().unlock();
    }


    @Override
    public void writeLock() {
        lock.writeLock().lock();
    }

    @Override
    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    @Override
    public Iterator<DataTreeNode> iterator() {
        return getNodeIterator();
    }

    @Override
    public Tree getTreeRoot() {
        return tree;
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator() {
        return getNodeIterator();
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String begin) {
        return getNodeIterator(begin);
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String begin, String end) {
        return getNodeIterator(begin, end);
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
}

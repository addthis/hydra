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
import com.addthis.hydra.store.db.IReadWeighable;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;


/**
 *         <p/>
 *         read only tree node that plays nice with ReadTree
 */
public class ReadTreeNode extends AbstractTreeNode implements IReadWeighable {

    /**
     * required for Codable. must be followed by an init() call.
     */
    public ReadTreeNode() {
    }

    public ReadTreeNode(String name, int weight) {
        this.name = name;
        setWeight(weight);
    }

    /**
     * Set by the tree when the node is deserialized since the name is kept in the key
     * and the decoder does not have a reference to the tree.
     *
     * @param tree The ReadTree this node belongs to
     * @param name The name/label/key of the node
     */
    protected void init(ReadTree tree, String name) {
        this.tree = tree;
        this.name = name;
    }

    //reference to the transient (in memory) tree object -- not serialized
    @Mem(estimate = false, size = 64)
    private ReadTree tree;

    //node's name (eg 'www.ianrules.com'). combined with its parent's nodedb (not stored here), forms this nodes
    //unique id
    protected String name;

    /**
     * Used by a few data attachment query methods to return fake nodes for reporting. Just clones this read-only
     * node with a new hits value (counter).
     *
     * @param val - new counter/hits value
     * @return fake/temp node
     */
    public DataTreeNode getCloneWithCount(long val) {
        ReadTreeNode tn = new ReadTreeNode();
        tn.name = name;
        tn.hits = val; //the count change
        tn.nodes = nodes;
        tn.nodedb = nodedb;
        tn.bits = bits;
        tn.data = data;
        tn.tree = tree;
        return tn;
    }

    public String toString() {
        return "TN[db=" + nodedb + ",n#=" + nodes + ",h#=" + hits + ",nm=" + name + ",bi=" + bits + "]";
    }

    public String getName() {
        return name;
    }

    @Override
    public void lease() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release() {
        throw new UnsupportedOperationException();
    }

    protected Integer nodeDB() {
        return nodedb;
    }

    /**
     * returns an iterator of this node's children
     */
    public ClosableIterator<DataTreeNode> getNodeIterator() {
        return nodedb == null ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb));
    }

    public ClosableIterator<DataTreeNode> getNodeIterator(int sampleRate) {
        return nodedb == null ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb, sampleRate));
    }

    /**
     * returns an iterator of this node's children that start with a given prefix
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String prefix) {
        if (prefix != null && prefix.length() > 0) {
            StringBuilder sb = new StringBuilder(prefix.substring(0, prefix.length() - 1));
            sb.append((char) (prefix.charAt(prefix.length() - 1) + 1));
            return getNodeIterator(prefix, sb.toString());
        } else {
            return new Iter(null);
        }
    }

    /**
     * returns an iterator of this node's children whose names are within the range
     * of from-to
     */
    public ClosableIterator<DataTreeNode> getNodeIterator(String from, String to) {
        return nodedb == null ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb, from, to));
    }

    public DataTreeNode getNode(String name) {
        return tree.getNode(this, name);
    }

    public ReadTreeNode getLeasedNode(String name) {
        throw new UnsupportedOperationException();
    }

    public boolean deleteNode(String node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean aliasTo(DataTreeNode node) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path) {
        throw new UnsupportedOperationException();
    }

    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, TreeNodeData> getDataMap() {
        return data;
    }

    // TODO concurrent broken -- data classes should be responsible for their
    // own get/update sync
    public DataTreeNodeActor getData(String key) {
        return data != null ? data.get(key) : null;
    }

    @Override
    public int getNodeCount() {
        return nodes;
    }

    @Override
    public void postDecode() {
        if (data != null) {
            for (TreeNodeData actor : data.values()) {
                actor.setBoundNode(this);
            }
        }
    }

    @Override
    public void preEncode() {
    }

    @Override
    public void setWeight(int weight) {
        bits = weight;
    }

    @Override
    public int getWeight() {
        return bits;
    }

    /**
     * TODO warning: not thread safe. sync around next(), hasNext() when
     * concurrency is required.
     */
    private final class Iter implements ClosableIterator<DataTreeNode> {

        private Range<DBKey, ReadTreeNode> range;
        private DataTreeNode next;

        private Iter(Range<DBKey, ReadTreeNode> range) {
            this.range = range;
            fetchNext();
        }

        public String toString() {
            return "Iter(" + range + "," + next + ")";
        }

        /**
         * Does not interact with node cache in tree
         */
        void fetchNext() {
            if (range != null) {
                next = null;
                if (range.hasNext()) {
                    Entry<DBKey, ReadTreeNode> tne = range.next();
                    next = tne.getValue();
                    ((ReadTreeNode) next).init(tree, tne.getKey().rawKey().toString());
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public DataTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DataTreeNode ret = next;
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
        return true;
    }

    @Override
    public void encodeUnlock() {
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
    public Iterator<DataTreeNode> iterator() {
        return getNodeIterator();
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator() {
        return getNodeIterator();
    }

    @Override
    public DataTree getTreeRoot() {
        return tree;
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String begin) {
        return getNodeIterator(begin);
    }

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String begin, String end) {
        return getNodeIterator(begin, end);
    }

    public ClosableIterator<DataTreeNode> getIterator(int sampleRate) {
        return getNodeIterator(sampleRate);
    }

    @Override
    public DataTreeNode getOrCreateNode(String name, DataTreeNodeInitializer init) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCounter() {
        return hits;
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
    public void setCounter(long val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] bytesEncode(long version) {
        throw new UnsupportedOperationException("ReadTreeNode cannot be encoded");
    }
}

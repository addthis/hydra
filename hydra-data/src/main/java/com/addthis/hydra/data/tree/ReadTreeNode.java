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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.MemoryCounter.Mem;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.IReadWeighable;

import com.google.common.base.Objects;


/**
 *         <p/>
 *         read only tree node that plays nice with ReadTree
 */
public class ReadTreeNode extends AbstractTreeNode implements IReadWeighable {

    //reference to the transient (in memory) tree object -- not serialized
    @Mem(estimate = false, size = 64)
    private ReadTree tree;

    /** node's name (eg 'www.ianrules.com'). Combines with its parent's nodedb (not provided), to form its unique id. */
    protected String name;

    /** required for CodecBin2. must be followed by an init() call. */
    public ReadTreeNode() {}

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

    @Override public String getName() {
        return name;
    }

    public long nodeDB() {
        return nodedb;
    }

    @Nullable @Override public DataTreeNode getNode(String name) {
        return tree.getNode(this, name);
    }

    @Override public Map<String, TreeNodeData> getDataMap() {
        return data;
    }

    @Nullable @Override public DataTreeNodeActor getData(String key) {
        return (data != null) ? data.get(key) : null;
    }

    @Override public long getCounter() {
        return hits;
    }

    @Override public int getNodeCount() {
        return nodes;
    }

    @Override public DataTree getTreeRoot() {
        return tree;
    }

    // the bits field is co-opted here to store the weight since we have no need for it in the query system

    @Override public void setWeight(int weight) {
        bits = weight;
    }

    @Override public int getWeight() {
        return bits;
    }

    @NotThreadSafe
    private final class Iter implements ClosableIterator<DataTreeNode> {

        private IPageDB.Range<DBKey, ReadTreeNode> range;
        private DataTreeNode next;

        private Iter(@Nullable IPageDB.Range<DBKey, ReadTreeNode> range) {
            this.range = range;
            fetchNext();
        }

        @Override public String toString() {
            return "Iter(" + range + "," + next + ")";
        }

        /** Does not interact with node cache in tree */
        void fetchNext() {
            if (range != null) {
                next = null;
                if (range.hasNext()) {
                    Map.Entry<DBKey, ReadTreeNode> tne = range.next();
                    next = tne.getValue();
                    ((ReadTreeNode) next).init(tree, tne.getKey().rawKey().toString());
                }
            }
        }

        @Override public boolean hasNext() {
            return next != null;
        }

        @Override public DataTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DataTreeNode ret = next;
            fetchNext();
            return ret;
        }

        @Override public void close() {
            if (range != null) {
                range.close();
                range = null;
            }
        }
    }

    @Override public Iterator<DataTreeNode> iterator() {
        return getIterator();
    }

    @Override public ClosableIterator<DataTreeNode> getIterator() {
        return !hasNodes() ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb));
    }

    @Override public ClosableIterator<DataTreeNode> getIterator(String prefix) {
        if ((prefix != null) && !prefix.isEmpty()) {
            StringBuilder sb = new StringBuilder(prefix.substring(0, prefix.length() - 1));
            sb.append((char) (prefix.charAt(prefix.length() - 1) + 1));
            return !hasNodes() ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb, prefix, sb.toString()));
        } else {
            return new Iter(null);
        }
    }

    @Override public ClosableIterator<DataTreeNode> getIterator(String from, String to) {
        return !hasNodes() ? new Iter(null) : new Iter(tree.fetchNodeRange(nodedb, from, to));
    }

    @Override public byte[] bytesEncode(long version) {
        throw new UnsupportedOperationException("ReadTreeNode cannot be encoded");
    }

    /** Returns a clone of this node, but with a new hits value. */
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

    @Override public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("nodedb", nodedb)
                      .add("nodes", nodes)
                      .add("hits", hits)
                      .add("weight", getWeight())
                      .toString();
    }
}

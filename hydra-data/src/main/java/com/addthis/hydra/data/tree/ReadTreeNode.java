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

import java.util.Iterator;
import java.util.Map;

import com.addthis.basis.util.MemoryCounter.Mem;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.IReadWeighable;


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
     */
    protected void initName(String name) {
        this.name = name;
    }

    protected void initTree(ReadTree tree) {
        this.name = name;
    }

    //reference to the transient (in memory) tree object -- not serialized
    @Mem(estimate = false, size = 64)
    @Codec.Set(codable = false)
    ReadTree tree;

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
    public ReadNode getCloneWithCount(long val) {
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

    @Override
    public String getName() {
        return name;
    }

    protected Integer nodeDB() {
        return nodedb;
    }

    /**
     * returns an iterator of this node's children
     */
    public Iterator<ReadNode> getNodeIterator() {
        if (nodedb == null) {
            return new NameSettingIteratorWrapper(null);
        } else {
            return new NameSettingIteratorWrapper(tree.fetchNodeRange(nodedb));
        }
    }

    public Iterator<ReadNode> getNodeIterator(int sampleRate) {
        if (nodedb == null) {
            return new NameSettingIteratorWrapper(null);
        } else {
            return new NameSettingIteratorWrapper(tree.fetchNodeRange(nodedb, sampleRate));
        }
    }

    /**
     * returns an iterator of this node's children that start with a given prefix
     */
    public Iterator<ReadNode> getNodeIterator(String prefix) {
        if ((prefix != null) && !prefix.isEmpty()) {
            StringBuilder sb = new StringBuilder(prefix.substring(0, prefix.length() - 1));
            sb.append((char) ((int) prefix.charAt(prefix.length() - 1) + 1));
            return getNodeIterator(prefix, sb.toString());
        } else {
            return new NameSettingIteratorWrapper(null);
        }
    }

    /**
     * returns an iterator of this node's children whose names are within the range
     * of from-to
     */
    public Iterator<ReadNode> getNodeIterator(String from, String to) {
        if (nodedb == null) {
            return new NameSettingIteratorWrapper(null);
        } else {
            return new NameSettingIteratorWrapper(tree.fetchNodeRange(nodedb, from, to));
        }
    }

    @Override
    public ReadNode getNode(String name) {
        return tree.getNode(this, name);
    }

    @Override
    public Map<String, TreeNodeData<?>> getDataMap() {
        return data;
    }

    @Override
    public TreeNodeData<?> getData(String key) {
        if (data != null) {
            return data.get(key);
        } else {
            return null;
        }
    }

    @Override
    public int getNodeCount() {
        return nodes;
    }

    @Override
    public void setWeight(int weight) {
        bits = weight;
    }

    @Override
    public int getWeight() {
        return bits;
    }

    public Iterator<ReadNode> iterator() {
        return getNodeIterator();
    }

    @Override
    public Iterator<ReadNode> getIterator() {
        return getNodeIterator();
    }

    @Override
    public ReadDataTree getTreeRoot() {
        return tree;
    }

    @Override
    public Iterator<ReadNode> getIterator(String begin) {
        return getNodeIterator(begin);
    }

    @Override
    public Iterator<ReadNode> getIterator(String begin, String end) {
        return getNodeIterator(begin, end);
    }

    public Iterator<ReadNode> getIterator(int sampleRate) {
        return getNodeIterator(sampleRate);
    }

    @Override
    public long getCounter() {
        return hits;
    }

    @Override
    public byte[] bytesEncode(long version) {
        throw new UnsupportedOperationException("ReadTreeNode cannot be encoded");
    }
}

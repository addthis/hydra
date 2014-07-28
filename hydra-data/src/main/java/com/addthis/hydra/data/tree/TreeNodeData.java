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

import java.util.Collection;
import java.util.List;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.binary.CodecBin2;
import com.addthis.codec.codables.BytesCodable;

/**
 * These classes are stored in TreeNodes.  New instances are
 * configured using TreeDataConfig objects which are stored
 * in the 'data' field of PathElements.
 */
@Pluggable("tree node data")
public abstract class TreeNodeData<C extends TreeDataParameters<?>> implements BytesCodable, DataTreeNodeActor {

    private static final CodecBin2 codec = CodecBin2.INSTANCE;

    /**
     * called from PathValue.processNodeUpdates() -> PathValue.processChild() -> TreeNode
     * .updateChildData() -> this.
     * implement data handling in child node.  called before parent nodeUpdate().
     */
    public abstract boolean updateChildData(DataTreeNodeUpdater state,
                                            DataTreeNode childNode,
                                            C conf);

    /**
     * override to track new children
     */
    public boolean updateParentNewChild(DataTreeNodeUpdater state, DataTreeNode parentNode,
                                        DataTreeNode childNode,
                                        List<TreeNodeDataDeferredOperation> deferredOps) {
        return false;
    }

    /**
     * called from PathValue.processNodeUpdates() -> TreeNode.updateParentData() -> this.
     * override to muck with the parent node after a child is processed (processChild()).
     * return true if node or data was altered.
     */
    public boolean updateParentData(DataTreeNodeUpdater state, DataTreeNode parentNode,
                                    DataTreeNode childNode,
                                    List<TreeNodeDataDeferredOperation> deferredOps) {
        return false;
    }

    /**
     * return a stored value to the query engine given a query key
     */
    public abstract ValueObject getValue(String key);

    /**
     * return a list of non-editable child nodes to the query engine given a query key
     */
    public Collection<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        return null;
    }

    /**
     * return valid value names
     */
    public Collection<String> getValueTypes() {
        return null;
    }

    /**
     * return types of synthetic nodes returned
     */
    public List<String> getNodeTypes() {
        return null;
    }

    /**
     * --- support for DataTreeNodeActor ---
     */

    private DataTreeNode node;

    public DataTreeNode getBoundNode() {
        return node;
    }

    @Override
    public void setBoundNode(DataTreeNode node) {
        this.node = node;
    }

    @Override
    public Collection<DataTreeNode> onNodeQuery(String option) {
        return getNodes(node, option);
    }

    @Override
    public ValueObject onValueQuery(String option) {
        return getValue(option);
    }

    @Override
    public byte[] bytesEncode(long version) {
        try {
            return codec.encode(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        try {
            codec.decode(this, b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

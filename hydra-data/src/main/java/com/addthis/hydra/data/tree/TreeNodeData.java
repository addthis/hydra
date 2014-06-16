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
import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.common.plugins.PluginReader;

/**
 * These classes are stored in TreeNodes.  New instances are
 * configured using TreeDataConfig objects which are stored
 * in the 'data' field of PathElements.
 */
@Codec.Set(classMapFactory = TreeNodeData.CMAP.class)
public abstract class TreeNodeData<T extends TreeDataParameters> implements Codec.BytesCodable {

    static final Codec.ClassMap cmap = new Codec.ClassMap() {
        @Override
        public String getClassField() {
            return "t";
        }
    };

    public static class CMAP implements Codec.ClassMapFactory {

        public Codec.ClassMap getClassMap() {
            return cmap;
        }
    }

    public static void registerClass(String key, Class<? extends TreeNodeData> clazz) {
        cmap.add(key, clazz);
    }

    /** register types */
    static {
        PluginReader.registerPlugin("-treedataparameters.classmap", cmap, TreeDataParameters.class);
        PluginReader.registerPlugin("-treenodedata.classmap", cmap, TreeNodeData.class);
    }

    private static final CodecBin2 codec = new CodecBin2();

    /**
     * called from PathValue.processNodeUpdates() -> PathValue.processChild() -> TreeNode.updateChildData() -> this.
     * implement data handling in child node.  called before parent nodeUpdate().
     */
    public abstract boolean updateChildData(DataTreeNodeUpdater state,
            DataTreeNode childNode, T conf);

    /**
     * called from PathValue.processNodeUpdates() -> PathValue.processChild() -> TreeNode.updateChildData() -> this.
     * implement data handling in child node.  called before parent nodeUpdate().
     *
     * This version explicitly casts the type so that it is clear what is happening at runtime.
     */
    public boolean updateChildDataUnsafe(DataTreeNodeUpdater state,
            DataTreeNode childNode, TreeDataParameters conf) {
        return updateChildData(state, childNode, (T) conf);
    }

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
    public Collection<ReadNode> getNodes(ReadNode parent, String key) {
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

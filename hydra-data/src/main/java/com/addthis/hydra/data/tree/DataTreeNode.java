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

public interface DataTreeNode extends ReadNode {

    /**
     * TODO temporary workaround.  MUST call ONLY for nodes retrieved via getOrCreateNode().
     */
    public void lease();

    /**
     * TODO temporary workaround.  MUST call ONLY for nodes retrieved via getOrCreateNode().
     */
    public void release();

    /**
     * atomically increment intrinsic counter
     */
    public void incrementCounter();

    /**
     * atomically increment intrinsic counter and return new value
     */
    public long incrementCounter(long val);

    /**
     * set value of intrinsic counter
     */
    public void setCounter(long val);

    public void writeLock();

    public void writeUnlock();

    /**
     * return node if it exists, do not create otherwise.
     * returned node is mutable.  MUST call release().
     */
    public DataTreeNode getLeasedNode(String name);

    /**
     * return node if it exists, create and return new otherwise.
     * returned node is read/write.  MUST call release() when complete to commit changes OR discard.
     */
    public DataTreeNode getOrCreateNode(String name, DataTreeNodeInitializer init);

    /**
     * create new node as alias (link) to another node;
     */
    public boolean aliasTo(DataTreeNode target);

    /**
     * @return true if node existed and was successfully deleted
     */
    public boolean deleteNode(String node);

    /**
     * TODO for immediate compatibility -- rethink this
     *
     * @param state
     * @param path
     */
    public void updateChildData(DataTreeNodeUpdater state, TreeDataParent path);

    /**
     * TODO for immediate compatibility -- rethink this
     *
     * @param state
     * @param child
     * @param isnew
     */
    public void updateParentData(DataTreeNodeUpdater state, DataTreeNode child, boolean isnew);

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String from, String to);

    @Override
    public ClosableIterator<DataTreeNode> getIterator(String prefix);

    @Override
    public ClosableIterator<DataTreeNode> getIterator();

    @Override
    public DataTreeNode getNode(String name);

    @Override
    public DataTree getTreeRoot();
}

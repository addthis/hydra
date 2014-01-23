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

import java.util.Map;

import com.addthis.basis.util.ClosableIterator;


public interface DataTreeNode extends Iterable<DataTreeNode> {

    /**
     * TODO temporary workaround.  MUST call ONLY for nodes retrieved via getOrCreateNode().
     */
    public void lease();

    /**
     * TODO temporary workaround.  MUST call ONLY for nodes retrieved via getOrCreateNode().
     */
    public void release();

    /**
     * @return this node's name (label) in the tree
     */
    public String getName();

    /**
     * @return tree that this node belongs to
     */
    public DataTree getTreeRoot();

    /**
     * @return number of iterable child nodes
     */
    public int getNodeCount();

    /**
     * @return value of intrinsic counter
     */
    public long getCounter();

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
     * @return data bound to this node/key
     */
    public DataTreeNodeActor getData(String key);

    /**
     * return node if it exists, do not create otherwise.
     * returned node is read only.  do not call release().
     */
    public DataTreeNode getNode(String name);

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
     * @return an iterator of all nodes in this branch
     */
    public ClosableIterator<DataTreeNode> getIterator();

    /**
     * @return an in iterator starting at first node with a matching <i>prefix</i>
     * and exclude all nodes that do not begin with <i>prefix</i>.  for example a
     * prefix of "abc" would <b>not</b> match names beginning with "abd".  for that
     * use case, use getIterator(from, to) where "to" could be null or a lexicographic
     * value &gt; "abd".
     */
    public ClosableIterator<DataTreeNode> getIterator(String prefix);

    /**
     * @param from optional beginning point (null = first)
     * @param to optional end point (null = last)
     * @return an in iterator starting at first node with a name &gt;= begin and &lt; to
     */
    public ClosableIterator<DataTreeNode> getIterator(String from, String to);

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

    Map<String, TreeNodeData> getDataMap();
}

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


public interface ReadNode {

    /**
     * @return this node's name (label) in the tree
     */
    public String getName();

    /**
     * @return tree that this node belongs to
     */
    public ReadDataTree getTreeRoot();

    /**
     * @return number of iterable child nodes
     */
    public int getNodeCount();

    /**
     * @return value of intrinsic counter
     */
    public long getCounter();

    /**
     * @return data bound to this node/key
     */
    public TreeNodeData<?> getData(String key);

    /**
     * return node if it exists, do not create otherwise.
     * returned node is read only.  do not call release().
     */
    public ReadNode getNode(String name);

    /**
     * @return an iterator of all nodes in this branch
     */
    public Iterator<? extends ReadNode> getIterator();

    /**
     * @return an in iterator starting at first node with a matching <i>prefix</i>
     * and exclude all nodes that do not begin with <i>prefix</i>.  for example a
     * prefix of "abc" would <b>not</b> match names beginning with "abd".  for that
     * use case, use getIterator(from, to) where "to" could be null or a lexicographic
     * value &gt; "abd".
     */
    public Iterator<? extends ReadNode> getIterator(String prefix);

    /**
     * @param from optional beginning point (null = first)
     * @param to optional end point (null = last)
     * @return an in iterator starting at first node with a name &gt;= begin and &lt; to
     */
    public Iterator<? extends ReadNode> getIterator(String from, String to);

    public Map<String, TreeNodeData<?>> getDataMap();
}

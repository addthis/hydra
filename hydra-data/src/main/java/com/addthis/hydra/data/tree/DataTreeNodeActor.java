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

import com.addthis.bundle.value.ValueObject;

public interface DataTreeNodeActor //extends ValueCustom, DataTreeNodeInitializer
{

    /**
     * called when actor is created or re-hydrated
     *
     * @param node node to which this actor is attached
     */
    public void setBoundNode(DataTreeNode node);

    /**
     * @return true if node was modified
     */
//  public boolean onParentUpdate(DataTreeNodeUpdater state);

    /**
     * @return true if node was modified
     */
//  public boolean onChildUpdate(DataTreeNodeUpdater state, DataTreeNode child);

    /**
     * @return true if node was modified
     */
//  public void onNewNode(DataTreeNode child);

    /**
     * @return list of related DataTreeNode
     */
    public Collection<DataTreeNode> onNodeQuery(String option);

    /**
     * @return requested value
     */
    public ValueObject onValueQuery(String option);
}

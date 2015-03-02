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

import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.tree.prop.DataLimitTop;
import com.addthis.hydra.data.tree.prop.DataSum;

/**
 * Data attachments are additional information that can be attached to tree nodes.
 * <p/>
 * <p>Data attachments can be purely informational (such as a {@link DataSum.Config sum}) or they
 * can affect
 * the rest of a tree.  For example a {@link DataLimitTop.Config limit.top} attachment limits how
 * many children
 * a node can have.</p>
 * <p/>
 * <p>When querying a data attachment use either the "${name}={parameters}" or
 * "%{name}={parameters}" syntax.
 * The "$" returns one or more values and the "%" returns a list of nodes.
 * The exact behavior of "$" and "%" are defined on a per data attachment basis.
 * For instance they may not be implemented for each type. The "%" notation may return either
 * fake nodes created during the query or pull real nodes from the tree.</p>
 *
 * @user-reference
 * @hydra-category Data Attachments
 * @hydra-doc-position 7
 */
@Pluggable("data-attachment")
public abstract class TreeDataParameters<T extends TreeNodeData<?>> implements Codable {

    public abstract T newInstance();

    public T newInstance(DataTreeNode boundTo) {
        T newInstance = newInstance();
        newInstance.setBoundNode(boundTo);
        return newInstance;
    }
}

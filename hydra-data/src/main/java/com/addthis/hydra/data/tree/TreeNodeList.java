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

import java.util.ArrayList;
import java.util.List;

/** */
@SuppressWarnings("serial")
public class TreeNodeList extends ArrayList<DataTreeNode> {

    public TreeNodeList(int size) {
        super(size);
    }

    public static TreeNodeList wrap(DataTreeNode node) {
        return new TreeNodeList(node);
    }

    private TreeNodeList(DataTreeNode tn) {
        super(1);
        add(tn);
    }

    public void addFirst(List<DataTreeNode> list) {
        if (list != null) {
            add(list.get(0));
        }
    }

    public void addAll(List<DataTreeNode> list) {
        if (list != null && list.size() > 0) {
            super.addAll(list);
        }
    }

    private void _addAll(List<DataTreeNode> list) {
        super.addAll(list);
    }

    public TreeNodeList getImmutable() {
        return new ImmutableTreeNodeList(this);
    }

    /** */
    private static final class ImmutableTreeNodeList extends TreeNodeList {

        public ImmutableTreeNodeList(List<DataTreeNode> list) {
            super(list.size());
            super._addAll(list);
        }

        @Override
        public void addFirst(List<DataTreeNode> list) {
            throw new RuntimeException("Immutable list");
        }

        @Override
        public void addAll(List<DataTreeNode> list) {
            throw new RuntimeException("Immutable list");
        }

        @Override
        public boolean add(DataTreeNode e) {
            throw new RuntimeException("Immutable list");
        }
    }
}

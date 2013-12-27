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
package com.addthis.hydra.data.tree.prop;

import java.util.NoSuchElementException;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeNode;


/**
 * phantom node created for reporting
 */
class VirtualTreeNode extends TreeNode {

    VirtualTreeNode(final String name, final long hits) {
        this(name, hits, null);
    }

    VirtualTreeNode(final String name, final long hits, final VirtualTreeNode children[]) {
        this.name = name;
        this.hits = hits;
        this.nodes = children != null ? children.length : 0;
        this.children = children;
    }

    private final VirtualTreeNode children[];

    @Override
    public ClosableIterator<DataTreeNode> getNodeIterator() {
        return new ITER();
    }

    @Override
    public ClosableIterator<DataTreeNode> getNodeIterator(String prefix) {
        ITER iter = new ITER();
        while (true) {
            VirtualTreeNode peek = iter.peek();
            if (peek == null || peek.name.startsWith(prefix)) {
                break;
            }
            iter.next();
        }
        return iter;
    }

    @Override
    public ClosableIterator<DataTreeNode> getNodeIterator(String from, String to) {
        ITER iter = new ITER();
        iter.end = to;
        while (true) {
            VirtualTreeNode peek = iter.peek();
            if (peek == null || peek.name.compareTo(from) >= 0) {
                break;
            }
            iter.next();
        }
        return iter;
    }

    @Override
    public TreeNode getNode(String name) {
        if (children == null || children.length == 0) {
            return null;
        }
        for (VirtualTreeNode vtn : children) {
            if (vtn.name.equals(name)) {
                return vtn;
            }
        }
        return null;
    }

    class ITER implements ClosableIterator<DataTreeNode> {

        int pos;
        String end;

        VirtualTreeNode peek() {
            return children != null && pos < children.length ? children[pos] : null;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            if (children != null && pos < children.length) {
                if (end != null) {
                    return children[pos].name.compareTo(end) < 0;
                }
                return true;
            }
            return false;
        }

        @Override
        public VirtualTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return children[pos++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

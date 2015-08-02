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

import java.io.IOException;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BooleanSupplier;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.hydra.store.db.CloseOperation;

public interface DataTree extends DataTreeNode {

    public void close() throws IOException;

    /**
     * Close the tree.
     *
     * @param cleanLog  if true then wait for the BerkeleyDB clean thread to finish.
     * @param operation optionally test or repair the berkeleyDB.
     */
    public default void close(boolean cleanLog, CloseOperation operation) throws IOException {
        close();
    }

    public default void sync() throws IOException {
        // intentionally empty
    }

    public default TreeConfig getAdvancedSettings() {
        throw new UnsupportedOperationException();
    }

    public default long getDBCount() {
        throw new UnsupportedOperationException();
    }

    public DataTreeNode getRootNode();

    @Override public default DataTreeNode getNode(String name) {
        return getRootNode().getNode(name);
    }

    /**
     * Delete from the backing storage all nodes that have been moved to be
     * children of the trash node where they are waiting deletion. Also delete
     * all subtrees of these nodes. After deleting each subtree then test
     * the provided {@param terminationCondition}. If it returns true then
     * stop deletion.
     *
     * @param terminationCondition invoked between subtree deletions to
     *                             determine whether to return from method.
     */
    public default void foregroundNodeDeletion(BooleanSupplier terminationCondition) {
        throw new UnsupportedOperationException("foregroundNodeDeletion");
    }

    // DataTreeNode implementations that delegate to the root node

    @Override public default ClosableIterator<DataTreeNode> getIterator() {
        return getRootNode().getIterator();
    }

    @Override public default ClosableIterator<DataTreeNode> getIterator(String prefix) {
        return getRootNode().getIterator(prefix);
    }

    @Override public default ClosableIterator<DataTreeNode> getIterator(String from, String to) {
        return getRootNode().getIterator(from, to);
    }

    @Override public default Iterator<DataTreeNode> iterator() {
        return getRootNode().iterator();
    }

    @Override public default String getName() {
        return getRootNode().getName();
    }

    @Override public default DataTree getTreeRoot() {
        return this;
    }

    @Override public default int getNodeCount() {
        return getRootNode().getNodeCount();
    }

    @Override public default long getCounter() {
        return getRootNode().getCounter();
    }

    @Override public default DataTreeNodeActor getData(String key) {
        return getRootNode().getData(key);
    }

    @Override public default Map<String, TreeNodeData> getDataMap() {
        return getRootNode().getDataMap();
    }
}

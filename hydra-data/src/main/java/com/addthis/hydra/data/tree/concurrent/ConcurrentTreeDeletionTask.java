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
package com.addthis.hydra.data.tree.concurrent;

import java.util.Map;
import java.util.function.BooleanSupplier;

import com.addthis.hydra.store.db.DBKey;

/**
 * Delete from the backing storage all nodes that have been moved to be
 * children of the trash node where they are waiting deletion. Also delete
 * all subtrees of these nodes. After deleting each subtree then test
 * the provided {@link ConcurrentTreeDeletionTask#terminationCondition}.
 * If it returns true then stop deletion.
 */
class ConcurrentTreeDeletionTask implements Runnable {

    private final ConcurrentTree dataTreeNodes;

    private final BooleanSupplier terminationCondition;

    public ConcurrentTreeDeletionTask(ConcurrentTree dataTreeNodes, BooleanSupplier terminationCondition) {
        this.dataTreeNodes = dataTreeNodes;
        this.terminationCondition = terminationCondition;
    }

    @Override
    public void run() {
        try {
            Map.Entry<DBKey, ConcurrentTreeNode> entry;
            do {
                entry = dataTreeNodes.nextTrashNode();
                if (entry != null) {
                    ConcurrentTreeNode node = entry.getValue();
                    dataTreeNodes.deleteSubTree(node, -1);
                    ConcurrentTreeNode prev = dataTreeNodes.source.remove(entry.getKey());
                    if (prev != null) {
                        dataTreeNodes.treeTrashNode.incrementCounter();
                    }
                }
            }
            while (entry != null && !terminationCondition.getAsBoolean());
        } catch (Exception ex) {
            ConcurrentTree.log.warn("{}", "Uncaught exception in concurrent tree background deletion thread", ex);
        }
    }
}

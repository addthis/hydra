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

import com.addthis.hydra.store.db.DBKey;

class BackgroundDeletionTask implements Runnable {

    private ConcurrentTree dataTreeNodes;

    public BackgroundDeletionTask(ConcurrentTree dataTreeNodes) {
        this.dataTreeNodes = dataTreeNodes;
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
            while (entry != null && !dataTreeNodes.closed.get());
        } catch (Exception ex) {
            ConcurrentTree.log.warn("{}", "Uncaught exception in concurrent tree background deletion thread", ex);
        }
    }
}

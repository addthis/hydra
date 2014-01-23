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

public interface DataTreeNodeInitializer {

    /**
     * Should be called only when a new tree node is created. No other
     * thread should be able to concurrently access the new node, and
     * any changes should be safely published without any work on the
     * part of implementations.
     *
     * Implementing classes should be aware of the imperfect contract
     * involving TreeMapState and the isnew flag used in some tree
     * processing logic so that they do not mistakenly prevent that
     * contract from being fulfilled (although they may wish to do
     * so intentionally, but may want to note as much).
     *
     * @param child newly created tree node
     */
    public void onNewNode(DataTreeNode child);
}

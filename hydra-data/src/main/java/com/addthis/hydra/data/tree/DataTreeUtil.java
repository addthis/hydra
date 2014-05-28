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

public class DataTreeUtil {

    public static final DataTreeNode pathLocateFrom(DataTreeNode node, String[] path) {
        int plen = path.length;
        for (int i = 0; i < plen; i++) {
            node = node.getNode(path[i]);
            if (node == null || i == plen - 1) {
                return node;
            }
        }
        return node;
    }

}

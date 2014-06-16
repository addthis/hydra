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
package com.addthis.hydra.task.output.tree;

import java.util.Collections;
import java.util.List;

import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;


/**
 * This {@link PathElement PathElement} <span class="hydra-summary">prints the contents of incoming
 * bundles to the console (standard output)</span>.
 *
 * @user-reference
 * @hydra-name debug
 */
public class PathDebug extends PathOp {

    /**
     * Optional prefix to the debugging output. Default is null.
     */
    @Codec.Set(codable = true)
    private String debug;

    @Override
    public List<DataTreeNode> getNextNodeList(TreeMapState state) {
        System.out.println("---> debug (" + debug + ") " + state.getBundle());
        return Collections.emptyList();
    }
}

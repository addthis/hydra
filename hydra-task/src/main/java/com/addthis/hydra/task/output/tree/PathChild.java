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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.DataTreeNode;

/**
 * @user-reference
 */
public final class PathChild extends PathElement {

    /**
     * Sequence of path elements that will be placed as layers of nodes in the tree.
     */
    @FieldConfig(codable = true, required = true)
    private PathElement[] layers;

    public PathChild() { }

    public PathChild(PathElement[] layers) {
        this.layers = layers;
    }

    @Override
    public String toString() {
        return "[PathChild layers=" + Arrays.toString(layers) + ']';
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        for (PathElement layer : layers) {
            layer.resolve(mapper);
        }
    }

    @Override
    public List<DataTreeNode> getNextNodeList(TreeMapState state) {
        List<DataTreeNode> res = state.processPath(layers);
        if (res == null) {
            res = new ArrayList<>(0);
        }
        if (!res.isEmpty() || op) {
            return res;
        } else {
            return null;
        }
    }
}

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

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.TreeNodeList;


/**
 * This {@link PathElement PathElement} <span class="hydra-summary">calls a named path element</span>.
 * <p/>
 * <p>This path element provides a mechanism for re-using the same path element
 * in multiple locations in the tree. In other words the same tree structure
 * can appear in several locations of the tree. Compare this path element
 * to the '{@link PathAlias alias}' path element. 'alias' does not create an additional copy
 * of the path element but instead places a link to an existing path element.</p>
 * <p/>
 * @user-reference
 * @hydra-name call
 */
public final class PathCall extends PathOp {

    @FieldConfig(codable = true)
    private TreeMapperPathReference target;

    /**
     * Name of the path element that is called.
     */
    @FieldConfig(codable = true)
    private String path;

    /**
     * work around the non-serialization of rule name in editor
     */
    @FieldConfig(codable = true, writeonly = true)
    private String printableRule;

    private PathElement ppath[];

    public PathCall() {
    }

    public PathCall(String path) {
        this.path = path;
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        if (path != null) {
            ppath = mapper.getPath(path);
        }
        if (target != null) {
            target.resolve(mapper);
            printableRule = target.ruleName();
        }
    }

    @Override
    public TreeNodeList getNextNodeList(TreeMapState state) {
        if (target != null) {
            state.dispatchRule(target);
        }
        if (ppath != null) {
            return state.processPath(ppath);
        }
        return TreeMapState.empty();
    }

}

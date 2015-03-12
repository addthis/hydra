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

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">performs a set of operations on the data</span>.
 * <p/>
 * <p>Use the 'filter' parameter to specify a
 * {@linkplain com.addthis.hydra.data.filter.bundle.BundleFilter bundle filter}
 * that will applied to the incoming bundles. This bundle filter is permitted
 * to modify the contents of the bundles.
 * <p/>
 * <p>Do not assign a value to the 'op' parameter of this path element.
 * It is assigned internally to a value of 'true' and must retain that value.
 * </p>
 * <p>Example:</p>
 * <pre>{type : "op", filter :
 *    {op : "field", from : "MATCH", filter : {op : "set", value : "good"}}
 * },
 * </pre>
 *
 * @user-reference
 */
public class PathOp extends PathElement {

    public PathOp() {
        op = true;
    }

    @Override
    public LeasedTreeNodeList getNextNodeList(final TreeMapState state) {
        return TreeMapState.empty();
    }
}

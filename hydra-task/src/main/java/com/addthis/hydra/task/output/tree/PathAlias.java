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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeInitializer;
import com.addthis.hydra.data.tree.DataTreeUtil;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">creates a reference to another existing node in the tree</span>.
 * <p/>
 * <p>The location of the target node is specified as a path
 * to the target node using the 'path' parameter. By default the traversal
 * to the target node begins at the root of the tree. If the 'peer' parameter
 * is set then the tree traversal begins at the current location. If the 'relativeUp'
 * parameter is a positive integer then the tree traversal begins with
 * an ancestor of the current location. The 'peer' and the 'relativeUp' parameters
 * are incompatible with each other: if 'peer' is set to true then the 'relativeUp'
 * parameter is ignored.</p>
 * <p>Compare this path element to the '{@link PathCall call}' path element.
 * 'call' does not create a reference to an existing path element but instead
 * creates a copy of a path element.</p>
 * </p>
 * <p>Example:</p>
 * <pre>
 *   { type : "alias",
 *     key : "DATE_YMD",
 *     data : {
 *       uid : {type : "count", ver : "hll", rsd:0.05, key : "UID"},
 *     },
 *     relativeUp : 1,
 *     path : [
 *       {type:"const", value:"ymd"},
 *       {type:"value", key:"DATE_YMD"},
 *     ]
 *   }</pre>
 *
 * @user-reference
 * @hydra-name alias
 */
public class PathAlias extends PathKeyValue {

    /**
     * Path traversal to the target node. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    protected PathValue path[];

    /**
     * When traversing the tree in search of the target node,
     * if this parameter is a positive integer then begin
     * the traversal this many levels higher than the
     * current location. If {@linkplain #peer} is true
     * then this parameter is ignored. Default is zero.
     */
    @Codec.Set(codable = true)
    protected int relativeUp;

    /**
     * When traversing the tree in search of the target node,
     * if this flag is true then begin the traversal at
     * current location. Default is false.
     */
    @Codec.Set(codable = true)
    protected boolean peer;

    /**
     * When true the alias acts like a filesystem hard link.
     * The target of the link is identified the first time
     * the alias is accessed. On subsequent access the target node
     * is retrieved immediately without traversing through the tree.
     * When this parameter is false the alias will re-traverse through
     * the tree to find the target of the link upon every access.
     * If you know the target node will never be deleted then you
     * should set this parameter to true for improved performance.
     * Default is false.
     */
    @Codec.Set(codable = true)
    protected boolean hard;

    /**
     * Default is false.
     */
    @Codec.Set(codable = true)
    protected int debug;

    /**
     * Default is null.
     */
    @Codec.Set(codable = true)
    private String debugKey;

    private int match;
    private int miss;

    public PathAlias() {
    }

    public PathAlias(PathValue path[]) {
        this.path = path;
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        for (PathValue pv : path) {
            pv.resolve(mapper);
        }
    }

    @Override
    public DataTreeNode getOrCreateNode(final TreeMapState state, final String name) {
        if (hard) {
            DataTreeNode node = state.getLeasedNode(name);
            if (node != null) {
                return node;
            }
        }
        String p[] = new String[path.length];
        for (int i = 0; i < p.length; i++) {
            p[i] = ValueUtil.asNativeString(path[i].getFilteredValue(state));
        }
        DataTreeNode alias = null;
        if (peer) {
            alias = DataTreeUtil.pathLocateFrom(state.current(), p);
        } else if (relativeUp > 0) {
            alias = DataTreeUtil.pathLocateFrom(state.peek(relativeUp), p);
        } else {
            alias = DataTreeUtil.pathLocateFrom(state.current().getTreeRoot(), p);
        }
        if (alias != null) {
            final DataTreeNode finalAlias = alias;
            DataTreeNodeInitializer init = new DataTreeNodeInitializer() {
                @Override
                public void onNewNode(DataTreeNode child) {
                    child.aliasTo(finalAlias);
                    state.onNewNode(child);
                }
            };
            if (debug > 0) {
                debug(true);
            }
            return state.getOrCreateNode(name, init);
        } else {
            if (debug > 0) {
                debug(false);
            }
            if (log.isDebugEnabled() || debug == 1) {
                log.warn("alias fail, missing " + Strings.join(p, " / "));
            }
            return null;
        }
    }

    protected synchronized void debug(boolean hit) {
        if (hit) {
            match++;
        } else {
            miss++;
        }
        if (match + miss >= debug) {
            log.warn("query[" + debugKey + "]: match=" + match + " miss=" + miss);
            match = 0;
            miss = 0;
        }
    }
}

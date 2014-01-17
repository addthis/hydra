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
 * This {@link com.addthis.hydra.task.output.tree.PathElement PathElement} <span class="hydra-summary">creates a reference to another existing node in the tree</span>.
 * <p/>
 * <p>Differs from {@link com.addthis.hydra.task.output.tree.PathAlias PathAlias} in that
 * the node is created regardless of whether the target exists when "create" is set to true.
 * And in this mode, if the target node is subsequently deleted, this node remains valid.</p>
 * Without create set, behaves similarly to the {@link com.addthis.hydra.task.output.tree.PathAlias PathAlias}
 * with the exception that it no longer honors deletion. This tradeoff is made to allow a performance
 * improvement of not lookup up the alias or creating an initializer when the node already exists.
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
public final class PathAlias2 extends PathKeyValue {

    /**
     * Path traversal to the target node. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private PathValue path[];

    /**
     * When traversing the tree in search of the target node,
     * if this parameter is a positive integer then begin
     * the traversal this many levels higher than the
     * current location. If {@linkplain #peer} is true
     * then this parameter is ignored. Default is zero.
     */
    @Codec.Set(codable = true)
    private int relativeUp;

    /**
     * When traversing the tree in search of the target node,
     * if this flag is true then begin the traversal at
     * current location. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean peer;

    /**
     * Default is false.
     */
    @Codec.Set(codable = true)
    private int debug;

    /**
     * Default is null.
     */
    @Codec.Set(codable = true)
    private String debugKey;

    private int match;
    private int miss;

    public PathAlias2() {
    }

    public PathAlias2(PathValue path[]) {
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
        DataTreeNode node = state.getLeasedNode(name);
        if (node != null) {
            return node;
        }
        if (create) {
            return state.getOrCreateNode(name, new DataTreeNodeInitializer() {
                @Override
                public void onNewNode(final DataTreeNode child) {
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
                    if (alias == null) {
                        if (debug > 0) {
                            debug(false);
                        }
                        if (log.isDebugEnabled() || debug == 1) {
                            log.warn("alias fail, missing " + Strings.join(p, " / "));
                        }
                    } else {
                        child.aliasTo(alias);
                    }
                }
            });
        } else {
            String p[] = new String[path.length];
            for (int i = 0; i < p.length; i++) {
                p[i] = ValueUtil.asNativeString(path[i].getPathValue(state));
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
    }

    private synchronized void debug(boolean hit) {
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

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

import javax.annotation.Nullable;

import java.util.stream.Stream;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.query.FieldValueList;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeUtil;
import com.addthis.hydra.data.tree.TreeNodeList;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">performs a query against
 * a pre-existing node in the data tree</span>.
 * <p/>
 * <p>The location of the target node is specified as a path
 * to the target node using the 'path' parameter. By default the traversal
 * to the target node begins at the root of the tree. If the 'relativeUp'
 * parameter is a positive integer then the tree traversal begins with
 * an ancestor of the current location.</p>
 * <p/>
 * <p>The query is performed against the target node and the query results are
 * injected into the current input bundle with specified named fields.</p>
 * <p/>
 * @user-reference
 * @hydra-name query
 */
public final class PathQuery extends PathOp {

    /**
     * Path traversal to the target node. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private PathValue[] path;

    /**
     * When traversing the tree in search of the target node,
     * if this parameter is a positive integer then begin
     * the traversal this many levels higher than the
     * current location. Default is zero.
     * Default is zero.
     */
    @FieldConfig(codable = true)
    private int relativeUp;

    @FieldConfig(codable = true)
    private PathQueryElement values;

    /**
     * If non-zero then begin emitting
     * debugging output after N bundles
     * have been observed. Default is zero.
     */
    @FieldConfig(codable = true)
    private int debug;

    /**
     * If 'debug' is non-zero then
     * append this prefix to the debugging
     * output. Default is null.
     */
    @FieldConfig(codable = true)
    private String debugKey;

    /**
     * If true then append all the matching results
     * into value arrays. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean accumulate;

    private int match;
    private int miss;

    public PathQuery() {
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        for (PathValue pv : path) {
            pv.resolve(mapper);
        }
        if (values != null) {
            values.resolve(mapper);
        }
    }

    @Nullable
    @Override
    public TreeNodeList getNextNodeList(TreeMapState state) {
        ValueObject[] paths = new ValueObject[path.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = path[i].getPathValue(state);
            if (paths[i] == null) {
                return null;
            }
        }
        Stream<DataTreeNode> references = null;
        boolean updated = false;
        try {
            if (relativeUp > 0) {
                references = DataTreeUtil.pathLocateFrom(state.peek(relativeUp), paths);
            } else {
                references = DataTreeUtil.pathLocateFrom(state.current().getTreeRoot(), paths);
            }
            updated = references.map(element -> evaluateNode(state, paths, element))
                                .reduce(false, (a, b) -> a || b);
        } finally {
            if (references != null) {
                references.close();
            }
        }
        return updated ? TreeMapState.empty() : null;
    }

    private boolean evaluateNode(TreeMapState state, ValueObject[] paths, DataTreeNode reference) {
        boolean updated = false;
        if (reference != null) {
            FieldValueList valueList = new FieldValueList(state.getFormat());
            if (values.update(valueList, reference, state) == 0) {
                return false;
            }
            if (accumulate) {
                updated = valueList.updateBundleWithAppend(state.getBundle());
            } else {
                updated = valueList.updateBundle(state.getBundle());
            }
            if (updated) {
                if (debug > 0) {
                    debug(true);
                }
            }
        } else {
            if (debug > 0) {
                debug(false);
            }
            if (log.isDebugEnabled() || (debug == 1)) {
                log.warn("query fail, missing {}", Strings.join(paths, " / "));
            }
        }
        return updated;
    }

    private synchronized void debug(boolean hit) {
        if (hit) {
            match++;
        } else {
            miss++;
        }
        if ((match + miss) >= debug) {
            log.warn("query[{}]: match={} miss={}", debugKey, match, miss);
            match = 0;
            miss = 0;
        }
    }
}

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

import java.util.List;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.TreeNodeList;
import com.addthis.hydra.data.util.Tokenizer;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">convert path values to file hierarchies</span>.
 * <p/>
 * <p>The 'key' parameter specifies the name of a bundle field. Each value
 * of this bundle field is processed as a file path. If the 'expand' parameter
 * is false then create a file root node and a node for each bundle field value.
 * If the 'expand' parameter is true then tokenize the path and create a nested node
 * hierarchy terminated with the file.
 * <p/>
 * <p>If the 'expand' parameter is true then properties associated with this path
 * element will be attributed to the last node created (the terminating file or directory).
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {name : "tpath", type : "file", key : "PATH", expand : true}
 * </pre>
 *
 * @user-reference
 * @hydra-name file
 */
public final class PathFile extends PathKeyValue {

    /**
     * If true then tokenize the path and create a nested node
     * hierarchy terminated with the file.
     * Default is false.
     */
    @Codec.Set(codable = true)
    private boolean expand;

    /**
     * If 'expand' is true then do something here. Default is null.
     */
    @Codec.Set(codable = true)
    private PathKeyValue each;

    /**
     * If 'expand' is true then optionally apply a
     * filter to each token produced by the path tokenizer.
     * Default is null.
     */
    @Codec.Set(codable = true)
    private ValueFilter tokenFilter;

    /**
     * If 'expand' is true then use the following string
     * as a path separator. This parameter is ignored if the
     * 'tokens' parameter is non-null. Default is "/".
     */
    @Codec.Set(codable = true)
    private String separator = "/";

    /**
     * If 'expand' is true then optionally
     * specify a path tokenizer. Otherwise
     * use the default tokenizer.
     * Default is null.
     */
    @Codec.Set(codable = true)
    private Tokenizer tokens;

    /**
     * If 'expand' is true then optionally specify
     * a maximum depth of the generated subtrees.
     * Default is zero.
     */
    @Codec.Set(codable = true)
    private int depth;

    /**
     * Default is "TEMP".
     */
    @Codec.Set(codable = true)
    private String tempKey = "TEMP";
    // treat file token same as dir token

    /**
     * If 'expand' is true then treat file
     * tokens and directory tokens in the same manner.
     * Default is false.
     */
    @Codec.Set(codable = true)
    private boolean same;

    /**
     * If 'expand' is true then cause filter returns
     * of null to terminate descent.
     * Default is false.
     */
    @Codec.Set(codable = true)
    private boolean termFilter;

    /**
     * Default is false.
     */
    @Codec.Set(codable = true)
    private boolean inheritData;

    private BundleField tempAccess;

    /** */
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        tempAccess = mapper.bindField(tempKey);
        if (each != null) {
            each.setKeyAccessor(tempAccess);
        }
        if (tokens != null) {
            separator = tokens.getSeparator();
        } else {
            if (separator == null || separator.length() > 1) {
                throw new RuntimeException("invalid separator : " + separator);
            }
            tokens = new Tokenizer().setSeparator(separator).setPacking(true);
        }
    }

    @Override
    public TreeNodeList processNodeUpdates(TreeMapState state, ValueObject ps) {
        ValueObject pv = getPathValue(state);
        String path = ValueUtil.asNativeString(pv);
        if (path.length() == 0 || path.equals(separator)) {
            return TreeMapState.empty();
        }
        int len = path.length();
        int start = 0;
        int end = len - 1;
        while (separator.indexOf(path.charAt(start)) >= 0 && start < end) {
            start++;
        }
        while (separator.indexOf(path.charAt(end)) >= 0 && end > start) {
            end--;
        }
        if (end - start < len) {
            path = path.substring(start, end + 1);
        }
        int lastsep = path.lastIndexOf(separator);
        String file = null;
        String root = null;
        if (lastsep < 0) {
            file = path;
        } else if (lastsep > 0) {
            file = path.substring(lastsep + 1);
            root = path.substring(0, lastsep);
        }
        if (expand) {
            TreeNodeList ret = null;
            boolean term = same;
            int pop = 0;
            if (root != null) {
                List<String> seg = tokens.tokenize(root);
                if (same) {
                    seg.add(file);
                }
                for (String dirString : seg) {
                    ValueObject dir = ValueFactory.create(dirString);
                    if (tokenFilter != null) {
                        dir = tokenFilter.filter(dir);
                        if (dir == null) {
                            if (termFilter) {
                                term = true;
                                break;
                            }
                            continue;
                        }
                    }
                    if (ValueUtil.isEmpty(dir)) {
                        continue;
                    }
                    PathValue value;
                    if (each != null) {
                        value = each;
                        state.getBundle().setValue(tempAccess, dir);
                    } else {
                        value = new PathValue(ValueUtil.asNativeString(dir));
                    }
                    if (inheritData) {
                        value.data = data;
                    }
                    ret = value.processNode(state);
                    if (ret != null) {
                        state.push(ret.get(0));
                        pop++;
                        if (depth > 0 && pop == depth) {
                            term = true;
                            break;
                        }
                    } else {
                        term = true;
                        break;
                    }
                }
            }
            if (!term && file != null) {
                ret = super.processNodeUpdates(state, ValueFactory.create(file));
            }
            while (pop-- > 0) {
                state.pop();
            }
            return ret;
        } else {
            if (root == null) {
                return new PathValue(file).processNode(state);
            }
            TreeNodeList proc = new PathValue(root).processNode(state);
            state.push(proc.get(0));
            TreeNodeList ret = super.processNodeUpdates(state, ValueFactory.create(file));
            state.pop();
            return ret;
        }
    }
}

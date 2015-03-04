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

import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.TreeNodeList;


/**
 * This {@link PathElement PathElement} <span class="hydra-summary">creates sibling path elements in the tree</span>.
 * <p/>
 * <p>In general, when a sequence (ie. an array) of path elements is specified,
 * then successor path elements in the sequence are placed as child elements
 * of the preceding path elements. When using the "branch" path element and
 * the "list" parameter, the "list" parameter will accept an array of
 * path element arrays. This will generate N path element siblings in the tree,
 * where N is the number of path element arrays that are specified.</p>
 * <p/>
 * <p>When using the "branch" path element and the "each" parameter,
 * this is equivalent to using the "list" parameter where all the
 * path element arrays contain one element, and the number of
 * path element arrays is equal to the size of the "each" parameter.</p>
 * <p/>
 * @user-reference
 */
public final class PathBranch extends PathElement {

    /**
     * Sequence of path elements that will be placed as siblings in the tree.
     */
    @FieldConfig(codable = true)
    private PathElement[] each;

    /**
     * Sequence of path element arrays that will be placed as siblings in the tree.
     */
    @FieldConfig(codable = true)
    private ArrayList<PathElement[]> list;

    private int count;

    public PathBranch() {
    }

    public PathBranch(PathElement[] each) {
        this.each = each;
    }

    @Override
    public String toString() {
        return "[PathEach each=" + (each != null ? Strings.join(each, ",") : "null") + " list=" + list + "]";
    }

    public void setEach(PathElement[] each) {
        if (count > 0) {
            throw new IllegalArgumentException("setEach only valid before resolving");
        }
        this.each = each;
    }

    public void setList(ArrayList<PathElement[]> list) {
        if (count > 0) {
            throw new IllegalArgumentException("setEach only valid before resolving");
        }
        this.list = list;
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        if (each != null) {
            for (PathElement pe : each) {
                pe.resolve(mapper);
            }
            count += each.length;
        }
        if (list != null) {
            for (PathElement[] pe : list) {
                for (PathElement p : pe) {
                    p.resolve(mapper);
                }
            }
            count += list.size();
        }
    }

    @Override
    public TreeNodeList getNextNodeList(TreeMapState state) {
        TreeNodeList res = new TreeNodeList(count);
        if (each != null) {
            for (PathElement anEach : each) {
                res.addAll(state.processPathElement(anEach));
            }
        }
        if (list != null) {
            for (PathElement[] pe : list) {
                res.addAll(state.processPath(pe));
            }
        }
        if (!res.isEmpty() || op) {
            return res;
        } else {
            return null;
        }
    }
}

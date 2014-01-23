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

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeNodeList;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">creates a single node with a specified value</span>.
 * <p/>
 * <p>Compare this path element to the "{@link PathKeyValue value}" path element. "value" creates one
 * or more nodes with a specified key.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>paths : {
 *   "ROOT" : [
 *     {type:"const", value:"date"},
 *     {type:"value", key:"DATE_YMD"},
 *     {type:"value", key:"DATE_HH"},
 *   ],
 * },</pre>
 *
 * @user-reference
 * @hydra-name const
 */
public class PathValue extends PathElement {

    public PathValue() {
    }

    public PathValue(String value) {
        this.value = value;
    }

    public PathValue(String value, boolean count) {
        this.value = value;
        this.count = count;
    }

    /**
     * Value to be stored in the constructed node.
     */
    @Codec.Set(codable = true)
    protected String value;

    @Codec.Set(codable = true)
    protected String set;

    @Codec.Set(codable = true)
    protected ValueFilter vfilter;

    @Codec.Set(codable = true)
    protected boolean sync;

    @Codec.Set(codable = true)
    protected boolean create = true;

    @Codec.Set(codable = true)
    protected boolean once = false;

    @Codec.Set(codable = true)
    protected boolean delete;

    @Codec.Set(codable = true)
    protected boolean push;

    @Codec.Set(codable = true)
    protected PathElement each;

    private ValueString valueString;
    private BundleField setField;

    public final ValueObject value() {
        if (valueString == null) {
            valueString = ValueFactory.create(value);
        }
        return valueString;
    }

    /**
     * override in subclasses
     */
    public ValueObject getPathValue(final TreeMapState state) {
        return value();
    }

    public final ValueObject getFilteredValue(final TreeMapState state) {
        ValueObject value = getPathValue(state);
        if (vfilter != null) {
            value = vfilter.filter(value);
        }
        return value;
    }

    @Override
    public void resolve(final TreeMapper mapper) {
        super.resolve(mapper);
        if (set != null) {
            setField = mapper.bindField(set);
        }
        if (each != null) {
            each.resolve(mapper);
        }
    }

    @Override
    public String toString() {
        return "PathValue[" + value + "]";
    }

    /**
     * prevent subclasses from overriding as this is not used from here on
     */
    @Override
    public final TreeNodeList getNextNodeList(final TreeMapState state) {
        ValueObject value = getFilteredValue(state);
        if (setField != null) {
            state.getBundle().setValue(setField, value);
        }
        if (ValueUtil.isEmpty(value)) {
            return op ? TreeMapState.empty() : null;
        }
        if (op) {
            return TreeMapState.empty();
        }
        TreeNodeList list = null;
        if (sync) {
            synchronized (this) {
                list = processNodeUpdates(state, value);
            }
        } else if (value != null) {
            list = processNodeUpdates(state, value);
        }
        return term ? null : list;
    }

    public DataTreeNode getOrCreateNode(TreeMapState state, String name) {
        if (create) {
            return state.getOrCreateNode(name, state);
        } else {
            return state.getLeasedNode(name);
        }
    }

    /**
     * override this in subclasses. the rules for this path element are to be
     * applied to the child (next node) of the parent (current node).
     */
    public TreeNodeList processNodeUpdates(TreeMapState state, ValueObject name) {
        TreeNodeList list = new TreeNodeList(1);
        int pushed = 0;
        if (name.getObjectType() == ValueObject.TYPE.ARRAY) {
            for (ValueObject o : name.asArray()) {
                if (o != null) {
                    pushed += processNodeByValue(list, state, o);
                }
            }
        } else if (name.getObjectType() == ValueObject.TYPE.MAP) {
            // TODO: TYPE.MAP has seen only minimal use.  These null
            // checks may be masking a problem.
            for (ValueMapEntry e : name.asMap()) {
                PathValue v = new PathValue(e.getKey(), count);
                if (v != null) {
                    TreeNodeList tnl = v.processNode(state);
                    if (tnl != null) {
                        state.push(tnl);
                        list.addAll(processNodeUpdates(state, e.getValue()));
                        state.pop();
                    }
                }
            }
        } else {
            pushed += processNodeByValue(list, state, name);
        }
        while (pushed-- > 0) {
            state.pop();
        }
        return list.size() > 0 ? list : null;
    }

    /**
     * can be called by subclasses to create/update nodes
     */
    public final int processNodeByValue(TreeNodeList list, TreeMapState state, ValueObject name) {
        if (each != null) {
            TreeNodeList next = state.processPathElement(each);
            if (push) {
                if (next.size() > 1) {
                    throw new RuntimeException("push and each are incompatible for > 1 return nodes");
                }
                if (next.size() == 1) {
                    state.push(next.get(0));
                    return 1;
                }
            } else {
                list.addAll(next);
            }
            return 0;
        }
        DataTreeNode parent = state.current();
        String sv = ValueUtil.asNativeString(name);
        if (delete) {
            parent.deleteNode(sv);
        }
        /** get db for parent node once we're past it (since it has children) */
        DataTreeNode child = getOrCreateNode(state, sv);
        boolean isnew = state.getAndClearLastWasNew();
        /** can be null if parent is deleted by another thread or if create == false */
        if (child == null) {
            return 0;
        }
        /* bail if only new nodes are required */
        if (once && (!isnew || child.getCounter() >= 1)) {
            return 0;
        }
        /** child node accounting and custom data updates */
        child.updateChildData(state, this);
        /** update node data accounting */
        parent.updateParentData(state, child, isnew);
        if (push) {
            state.push(child);
            return 1;
        } else {
            list.add(child);
            return 0;
        }
    }
}

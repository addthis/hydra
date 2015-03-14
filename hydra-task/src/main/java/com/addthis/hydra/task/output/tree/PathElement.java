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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeDataParent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * TODO clean up preCheck in element, event*
 * TODO add alias nodes/map that use 'hard' links across the tree for cross-indexes (doesn't PathAlias do this?)
 * TODO extend feature support (on/off) to data elements on nodes
 */

/**
 * A path element defines a component of the tree structure.
 * Depending on the path element it will define one or more nodes of the tree.</p>
 * <p/>
 * @user-reference
 * @hydra-category Path Elements
 * @hydra-doc-position 6
 * @exclude-fields name, count, op, term, feature, featureOff, data, filter, profileCalls, profileTime, disabled
 */
@Pluggable("path element")
public abstract class PathElement implements Codable, TreeDataParent {

    protected static final Logger log = LoggerFactory.getLogger(PathElement.class);
    protected static final boolean debug = System.getProperty("hydra.path.debug", "0").equals("1");
    protected static final boolean defCountHit = System.getProperty("hydra.default.counthit", "1").equals("1");


    public static final HashSet<String> featureSet = new HashSet<>();

    /**
     * If non-null then a parent node is inserted
     * into the path. The parent node is a "{@link PathValue const}" node
     * with a value that is equal to the value of this parameter.
     */
    @FieldConfig(codable = true)
    protected String name;

    /**
     * If true then record the number of bundles
     * that are processed by this path element.
     * Default is "hydra.default.counthit" configuration value
     * (as either "1" or "0") or true.
     */
    @FieldConfig(codable = true)
    protected boolean count = defCountHit;

    /**
     * If true then continue processing child path elements
     * even when the current path element does not have
     * any more data to process. Default is false.
     */
    @FieldConfig(codable = true)
    protected boolean op;

    /**
     * If true then do not process any subsequent path
     * elements in the enclosing sequence of path elements.
     * Default is false.
     */
    @FieldConfig(codable = true)
    protected boolean term;

    /**
     * The set of features for this path element.
     * If this set contains one or more elements that
     * are not included in the {@link TreeMapper#features features}
     * of the tree then disable this path element.
     */
    @FieldConfig(codable = true)
    protected HashSet<String> feature;

    /**
     * The set of features to disable this path element.
     * If this set contains one or more elements that
     * are included in the {@link TreeMapper#features features}
     * of the tree then disable this path element.
     */
    @FieldConfig(codable = true)
    protected HashSet<String> featureOff;

    /**
     * Definition of the data attachments.
     * Consists of a mapping from the name of a data attachment
     * to the structure of the data attachment.
     */
    @SuppressWarnings("unchecked")
    @FieldConfig(codable = true)
    protected HashMap<String, TreeDataParameters> data;

    /**
     * Optional {@linkplain BundleFilter bundle filter} to apply on incoming bundles.
     * Only bundles that pass the filter will be processed.
     * Default is null.
     */
    @FieldConfig(codable = true)
    protected BundleFilter filter;

    private PathValue label;
    @FieldConfig(codable = true, writeonly = true)
    private AtomicLong profileCalls;
    @FieldConfig(codable = true, writeonly = true)
    private AtomicLong profileTime;
    @FieldConfig(codable = true, writeonly = true)
    private boolean disabled;

    public PathElement() {
    }

    public String toString() {
        return this.getClass().getSimpleName();
    }

    public static String intern(String s) {
        return s != null ? s.intern() : null;
    }

    private final void initProfile() {
        if (profileCalls == null || profileTime == null) {
            clearProfile();
        }
    }

    public void clearProfile() {
        profileCalls = new AtomicLong(0);
        profileTime = new AtomicLong(0);
    }

    public long getProfileCalls() {
        initProfile();
        return profileCalls.get();
    }

    public long getProfileTime() {
        initProfile();
        return profileTime.get();
    }

    public void updateProfile(long time) {
        initProfile();
        profileCalls.incrementAndGet();
        profileTime.addAndGet(time);
    }

    /**
     * resolve one-bind bindings (done post-parsing from Config)
     */
    public void resolve(final TreeMapper mapper) {
        if (feature != null) {
            for (String f : feature) {
                if (!featureSet.contains(f)) {
                    disabled = true;
                    log.warn("feature disabled " + this + ", missing '" + f + "'");
                    break;
                }
            }
        }
        if (featureOff != null) {
            for (String f : featureOff) {
                if (featureSet.contains(f)) {
                    disabled = true;
                    log.warn("feature disabled " + this + ", enabled '" + f + "'");
                    break;
                }
            }
        }
        if (name != null) {
            label = new PathValue(intern(name));
            label.count = count;
        }
    }

    public final boolean isOp() {
        return op;
    }

    /**
     * wrapper that calls getPathValue to prevent multiple calls
     */
    public final List<DataTreeNode> processNode(final TreeMapState state) {
        if (debug) {
            log.warn("processNode<{}>", this);
        }
        List<DataTreeNode> list = null;
        if (filter == null || filter.filter(state.getBundle())) {
            if (label != null) {
                state.push(label.processNode(state));
                list = getNextNodeList(state);
                state.pop().release();
            } else {
                list = getNextNodeList(state);
            }
        }
        if (term) {
            if (list != null) {
                list.forEach(DataTreeNode::release);
            }
            return null;
        } else {
            if (op) {
                if (list != null) {
                    list.forEach(DataTreeNode::release);
                }
                return TreeMapState.empty();
            } else {
                return list;
            }
        }
    }

    /**
     * override in subclasses
     *
     * @return list of child nodes of current node to process next
     */
    public abstract List<DataTreeNode> getNextNodeList(final TreeMapState state);

    public final PathElement label() {
        return label;
    }

    @Override
    public final boolean countHits() {
        return count;
    }

    @SuppressWarnings("unchecked")
    @Override
    public HashMap<String, TreeDataParameters> dataConfig() {
        return data;
    }

    @SuppressWarnings("unchecked")
    public PathElement setData(final HashMap<String, TreeDataParameters> data) {
        this.data = data;
        return this;
    }

    public boolean disabled() {
        return disabled;
    }
}

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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeInitializer;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@SuppressWarnings("serial")
public final class TreeMapState implements DataTreeNodeUpdater, DataTreeNodeInitializer, BundleFactory, BundleFormatted {

    private static final Logger log = LoggerFactory.getLogger(TreeMapState.class);

    /**
     * test harness constructor
     */
    public TreeMapState(Bundle p) {
        this.bundle = p;
        this.path = null;
        this.processor = null;
        this.stack = null;
        this.profiling = false;
    }

    /** */
    public TreeMapState(TreeMapper processor, DataTreeNode rootNode, PathElement[] path, Bundle bundle) {
        this.path = path;
        this.bundle = bundle;
        this.processor = processor;
        this.countValue = 1;
        this.stack = new LinkedList<>();
        this.profiling = processor != null && processor.isProfiling();
        push(rootNode);
        process();
    }

    private final LinkedList<DataTreeNode> leases = new LinkedList<>();
    private final LinkedList<DataTreeNode> stack;
    private final TreeMapper processor;
    private final PathElement[] path;
    private final Bundle bundle;
    private final boolean profiling;

    private boolean lastWasNew;
    private int touched;
    private int countValue;

    public void addLeasedNode(DataTreeNode tn) {
        leases.push(tn);
    }

    @Override
    public int getCountValue() {
        return countValue;
    }

    public void setCountValue(int v) {
        countValue = v;
    }

    public boolean getAndClearLastWasNew() {
        boolean ret = lastWasNew;
        lastWasNew = false;
        return ret;
    }

    public DataTreeNode getLeasedNode(String key) {
        DataTreeNode tn = current().getLeasedNode(key);
        if (tn != null) {
            addLeasedNode(tn);
        }
        return tn;
    }

    public DataTreeNode getOrCreateNode(String key, DataTreeNodeInitializer init) {
        DataTreeNode tn = current().getOrCreateNode(key, init);
        if (tn != null) {
            addLeasedNode(tn);
        }
        return tn;
    }

    public DataTreeNode pop() {
        return stack.pop();
    }

    public void push(List<DataTreeNode> tnl) {
        if (tnl.size() == 1) {
            push(tnl.get(0));
        } else {
            throw new RuntimeException("unexpected response: " + tnl.size() + " -> " + tnl);
        }
    }

    public void push(DataTreeNode tn) {
        stack.push(tn);
    }

    /** */
    public DataTreeNode peek(int back) {
        return (stack.size() > back) ? stack.get(back) : null;
    }

    /** */
    public DataTreeNode current() {
        return stack.peek();
    }

    @Override
    public Bundle getBundle() {
        return bundle;
    }

    /** */
    public int touched() {
        return touched;
    }

    /**
     * used by path elements to schedule delivering the current bundle to a new
     * rule, usually as part of conditional processing. currently called from
     * PathCall and PathEachChild
     */
    public void dispatchRule(TreeMapperPathReference t) {
        if ((t != null) && (bundle != null) && (processor != null)) {
            processor.processBundle(bundle, t);
        }
    }

    /**
     * called exclusively from Hydra.processRule()
     */
    public void process() {
        try {
            List<DataTreeNode> list = processPath(path, 0);
            if (list.isEmpty()) {
                log.warn("proc FAIL " + list);
                log.warn(".... PATH " + Strings.join(path, " // "));
                log.warn(".... PACK " + bundle);
            }
            // release lease list
            while (!leases.isEmpty()) {
                leases.pop().release();
            }
        } finally {
            // there's something odd going on with leases getting new
            // objects inserted after process() exits
            leases.clear();
        }
    }

    /**
     * called from PathCall.processNode(), PathCombo.processNode() and
     * PathEach.processNode()
     */
    public List<DataTreeNode> processPath(PathElement[] path) {
        return processPath(path, 0);
    }

    /** */
    private List<DataTreeNode> processPath(PathElement[] path, int index) {
        if ((path == null) || (path.length <= index)) {
            return null;
        }
        List<DataTreeNode> nodes = processPathElement(path[index]);
        if ((nodes != null) && nodes.isEmpty()) {
            // we get here from op elements, each elements, etc
            if ((index + 1) < path.length) {
                return processPath(path, index + 1);
            }
            return null;
        }
        List<DataTreeNode> ret = nodes;
        if (nodes != null) {
            if ((index + 1) < path.length) {
                for (DataTreeNode tn : nodes) {
                    push(tn);
                    ret = processPath(path, index + 1);
                    pop();
                }
            }
        }
        return ret;
    }

    /**
     * called from this.processPath(), PathEach.processNode() and
     * PathSplit.processNode()
     */
    public List<DataTreeNode> processPathElement(PathElement pe) {
        if (profiling) {
            long mark = profiling ? System.nanoTime() : 0;
            List<DataTreeNode> list = processPathElementProfiled(pe);
            processor.updateProfile(pe, System.nanoTime() - mark);
            return list;
        } else {
            return processPathElementProfiled(pe);
        }
    }

    private List<DataTreeNode> processPathElementProfiled(PathElement pe) {
        if (pe.disabled()) {
            return Collections.emptyList();
        }
        List<DataTreeNode> list = pe.processNode(this);
        if (list != null) {
            touched += list.size();
        }
        return list;
    }

    @Override
    public void onNewNode(DataTreeNode child) {
        lastWasNew = true;
    }

    @Override
    public Bundle createBundle() {
        return processor.createBundle();
    }

    @Override
    public BundleFormat getFormat() {
        return processor.getFormat();
    }
}

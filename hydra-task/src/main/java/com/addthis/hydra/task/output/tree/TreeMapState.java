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

import java.util.LinkedList;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.hydra.common.util.CloseTask;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeInitializer;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeNodeList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@SuppressWarnings("serial")
public final class TreeMapState implements DataTreeNodeUpdater, DataTreeNodeInitializer, BundleFactory, BundleFormatted {

    private static final Logger log = LoggerFactory.getLogger(TreeMapState.class);
    private static final int debug = Integer.parseInt(System.getProperty("hydra.process.debug", "0"));
    private static final boolean debuglist = System.getProperty("hydra.process.debuglist", "0").equals("1");
    private static final boolean debugthread = System.getProperty("hydra.process.debugthread", "0").equals("1");
    private static final TreeNodeList empty = new TreeNodeList(0).getImmutable();

    static {
        if (debuglist) {
            log.warn("ENABLED debuglist");
        }
        if (debugthread) {
            log.warn("ENABLED debugthread");
        }
    }

    /**
     * test harness constructor
     */
    public TreeMapState(Bundle p) {
        this.bundle = p;
        this.path = null;
        this.processor = null;
        this.stack = null;
        this.thread = null;
        this.profiling = false;
    }

    /** */
    public TreeMapState(TreeMapper processor, DataTreeNode rootNode, PathElement[] path, Bundle bundle) {
        this.path = path;
        this.bundle = bundle;
        this.processor = processor;
        this.countValue = 1;
        this.stack = new LinkedList<>();
        this.thread = Thread.currentThread();
        this.profiling = processor != null ? processor.isProfiling() : false;
        push(rootNode);
    }

    private final LinkedList<DataTreeNode> leases = debuglist ? new DebugList() : new LinkedList<DataTreeNode>();
    private final LinkedList<DataTreeNode> stack;
    private final TreeMapper processor;
    private final PathElement[] path;
    private final Bundle bundle;
    private final Thread thread;
    private final boolean profiling;

    private boolean lastWasNew;
    private int touched;
    private int countValue;

    private void checkThread() {
        if (Thread.currentThread() != thread) {
            throw new RuntimeException("invalid accessing thread " + Thread.currentThread() + " != " + thread);
        }
    }

    public static TreeNodeList empty() {
        return empty;
    }

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
        if (debugthread) {
            checkThread();
        }
        return stack.pop();
    }

    public int getNodeCount() {
        return current().getNodeCount();
    }

    public void push(TreeNodeList tnl) {
        if (tnl.size() == 1) {
            push(tnl.get(0));
        } else {
            throw new RuntimeException("unexpected response: " + tnl.size() + " -> " + tnl);
        }
    }

    public void push(DataTreeNode tn) {
        if (debugthread) {
            checkThread();
        }
        stack.push(tn);
    }

    /** */
    public long getBundleTime() {
        throw new RuntimeException("fix me");
    }

    public void setBundleTime(long time) {
        throw new RuntimeException("fix me");
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
        if (t != null && bundle != null && processor != null) {
            processor.processBundle(bundle, t);
        } else if (debug > 0) {
            log.warn("Proc Rule Dispatch DROP " + t + " b/c p=" + bundle + " rp=" + processor);
        }
    }

    public void process() {
        try {
            TreeNodeList list = processPath(path, 0);
            if (debug > 0 && (list == null || list.size() == 0)) {
                log.warn("proc FAIL " + list);
                log.warn(".... PATH " + Strings.join(path, " // "));
                log.warn(".... PACK " + bundle);
            }
            // release lease list
            while (leases.size() > 0) {
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
    public TreeNodeList processPath(PathElement[] path) {
        return processPath(path, 0);
    }

    /** */
    private TreeNodeList processPath(PathElement[] path, int index) {
        if (path == null || path.length <= index) {
            return null;
        }
        TreeNodeList nodes = processPathElement(path[index]);
        if (nodes == empty || (nodes != null && nodes.size() == 0)) {
            // we get here from op elements, each elements, etc
            if (index + 1 < path.length) {
                return processPath(path, index + 1);
            }
            return null;
        }
        TreeNodeList ret = nodes;
        if (nodes != null && nodes.size() > 0) {
            if (index + 1 < path.length) {
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
    public TreeNodeList processPathElement(PathElement pe) {
        if (profiling) {
            long mark = System.nanoTime();
            TreeNodeList list = processPathElementProfiled(pe);
            processor.updateProfile(pe, System.nanoTime() - mark);
            return list;
        } else {
            return processPathElementProfiled(pe);
        }
    }

    private TreeNodeList processPathElementProfiled(PathElement pe) {
        if (pe.disabled()) {
            return empty();
        }
        if (debugthread) {
            checkThread();
        }
        TreeNodeList list = pe.processNode(this);
        if (list != null) {
            touched += list.size();
        }
        return list;
    }

    /**
     * for debugging
     */
    static class DebugList extends LinkedList<DataTreeNode> {

        @Override
        public void finalize() {
            if (size() > 0) {
                System.out.println("!!!! lease finalize() size == " + this);
                System.exit(1);
            }
        }
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

    public boolean processorClosing() { return processor.isClosing(); }
}

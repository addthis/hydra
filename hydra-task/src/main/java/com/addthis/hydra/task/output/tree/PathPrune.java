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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.JitterClock;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.annotations.Time;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.prop.DataTime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Runnables;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">eliminates child nodes based on a time stamp</span>.
 * <p/>
 * <p>The time stamp is extracted from a property of the node with the property key specified by
 * {@link #timePropKey timePropKey}. Nodes with time stamps that are older than {@link #ttl ttl}
 * milliseconds are removed.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *  {prune {ttl:2592000000, relativeDown:1}}
 * </pre>
 *
 * @user-reference
 */
public class PathPrune extends PathElement {
    private static final Logger logger = LoggerFactory.getLogger(PathPrune.class);

    /** Maximum age in milliseconds. */
    @Time(TimeUnit.MILLISECONDS) @FieldConfig private long ttl;

    /** Property key name for extracting the time stamp of a node. Default is "time". */
    @FieldConfig private String timePropKey = "time";
    @FieldConfig private boolean ignoreMissingTimeProp = false;

    /**
     * If true then delete all nodes at the leaf level of matching.
     * Otherwise perform the default date matching behavior.
     * Default is false.
     */
    @FieldConfig private boolean allLeaves = false;

    /**
     * When traversing the tree in search of the nodes to prune, if this parameter is a positive integer then begin
     * the traversal this many levels lower than the current location. Default is zero.
     */
    @FieldConfig private int relativeDown = 0;

    /**
     * Optionally specify a path for traversal before pruning is initiated.
     * This parameter is incompatible the relativeDown parameter. The recognized
     * path types are "*" for matching all values, "{{date}}" for date matching,
     * and "foo" for matching a specific value.
     */
    @Nullable private ImmutableList<String> treePath;

    /**
     * If true then terminate the pruning process when the job is shutting down.
     * Default is false. Note that specifying a prune in the "post" section and
     * setting preempt to true can prevent job pruning from happening if the
     * job always terminates when its maximum runtime is reached.
     * Consider specifying a prune in the "pre" section and setting preempt to true.
     */
    @FieldConfig private boolean preempt = false;

    /**
     * If non-null then parse the name of each node using the provided
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat
     * .html">DateTimeFormat</a>. Default is null. By default the
     * parser will use the default time zone. To change the time zone
     * use the "timezone" field.
     */
    @Nullable private final DateTimeFormatter nameFormat;

    private Splitter SLASH_SPLITTER = Splitter.on('/').omitEmptyStrings();

    public PathPrune(@Nullable @JsonProperty("nameFormat") String nameFormat,
                     @Nullable @JsonProperty("timezone") String timezone,
                     @Nullable @JsonProperty("treePath") String treePath) {
        if (nameFormat != null && timezone != null) {
            this.nameFormat = DateTimeFormat.forPattern(nameFormat).withZone(DateTimeZone.forID(timezone));
        } else if (nameFormat != null) {
            this.nameFormat = DateTimeFormat.forPattern(nameFormat);
        } else {
            this.nameFormat = null;
        }
        if ((treePath != null) && (relativeDown != 0)) {
            throw new IllegalStateException("cannot use both treePath and relativeDown parameters");
        }
        if (treePath != null) {
            this.treePath = ImmutableList.copyOf(SLASH_SPLITTER.splitToList(treePath));
        }
    }

    // Is it better to try to do the pruning in this method or
    // whatever is getting the TreeNodeList back?
    @Override
    public List<DataTreeNode> getNextNodeList(final TreeMapState state) {
        List<DataTreeNode> result = TreeMapState.empty();
        long now = JitterClock.globalTime();
        DataTreeNode root = state.current();
        if (preempt && (state.processorClosing() || expensiveShutdownTest())) {
            log.info("Path pruning is not executing due to JVM shutdown.");
            return result;
        }
        findAndPruneChildren(state, root, now, relativeDown, treePath);
        return result;
    }

    public void findAndPruneChildren(final TreeMapState state, final DataTreeNode root, long now, int depth, List<String> treePaths) {
        if ((depth == 0) && ((treePaths == null) || (treePaths.size() == 0))) {
            pruneChildren(state, root, now);
        } else if (treePaths != null) {
            String current = treePaths.get(0);
            List<String> next = treePaths.subList(1, treePaths.size());
            if ("*".equals(current)) {
                ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
                try {
                    while (keyNodeItr.hasNext() && !(preempt && state.processorClosing())) {
                        findAndPruneChildren(state, keyNodeItr.next(), now, 0, next);
                    }
                } finally {
                    keyNodeItr.close();
                }
            } else if ("{{date}}".equals(current)) {
                ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
                try {
                    while (keyNodeItr.hasNext() && !(preempt && state.processorClosing())) {
                        DataTreeNode treeNode = keyNodeItr.next();
                        long nodeTime = getNodeTime(treeNode);
                        if ((nodeTime > 0) && ((now - nodeTime) > ttl)) {
                            findAndPruneChildren(state, treeNode, now, 0, next);
                        }
                    }
                } finally {
                    keyNodeItr.close();
                }
            } else {
                DataTreeNode nextNode = root.getNode(current);
                if (nextNode != null) {
                    findAndPruneChildren(state, nextNode, now, 0, next);
                }
            }
        } else {
            ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
            try {
                while (keyNodeItr.hasNext() && !(preempt && state.processorClosing())) {
                    findAndPruneChildren(state, keyNodeItr.next(), now, depth - 1, null);
                }
            } finally {
                keyNodeItr.close();
            }
        }
    }

    public void pruneChildren(final TreeMapState state, final DataTreeNode root, long now) {
        ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
        int deleted = 0;
        int kept = 0;
        int total = 0;
        try {
            while (keyNodeItr.hasNext() && !(preempt && state.processorClosing())) {
                DataTreeNode treeNode = keyNodeItr.next();
                boolean delete;
                if (allLeaves) {
                    delete = true;
                } else {
                    long nodeTime = getNodeTime(treeNode);
                    delete = ((nodeTime > 0) && ((now - nodeTime) > ttl));
                }
                if (delete) {
                    root.deleteNode(treeNode.getName());
                    deleted++;
                } else {
                    kept++;
                }
                total++;
                if ((total % 100000) == 0) {
                    logger.info("Iterating through children of {}, deleted: {} kept: {}", root.getName(), deleted,
                                kept);
                }
            }
        } finally {
            logger.info("Iterated through children of {}, deleted: {} kept: {}", root.getName(), deleted, kept);
            keyNodeItr.close();
        }
    }

    /**
     * It is bad practice use the shutdown hook mechanism as a way
     * to test whether the JVM is shutting down. However if we use
     * this method exactly once then it is useful when writing tests
     * to ensure that zero prune operations occur during shutdown.
     *
     * @return true iff the jvm is shutting down
     */
    private static boolean expensiveShutdownTest() {
        try {
            Thread shutdownHook = new Thread(Runnables.doNothing(), "Path prune shutdown hook");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            return true;
        }
        return false;
    }

    @VisibleForTesting long getNodeTime(DataTreeNode treeNode) {
        if (nameFormat != null) {
            return nameFormat.parseMillis(treeNode.getName());
        } else {
            DataTime dt = (DataTime) treeNode.getData(timePropKey);
            if (dt == null) {
                if (ignoreMissingTimeProp) {
                    return -1;
                }
                throw new RuntimeException("missing time attachment with key " + timePropKey);
            } else {
                return dt.last();
            }
        }
    }
}

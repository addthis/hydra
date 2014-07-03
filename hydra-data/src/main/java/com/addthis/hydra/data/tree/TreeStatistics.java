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
package com.addthis.hydra.data.tree;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.ClosableIterator;

import com.addthis.codec.util.CodableStatistics;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.kv.ReadExternalPagedStore;
import com.addthis.hydra.store.kv.metrics.ExternalPagedStoreMetrics;
import com.addthis.hydra.util.Histogram;

import com.yammer.metrics.stats.Snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class TreeStatistics {

    private final Logger log = LoggerFactory.getLogger(TreeStatistics.class);

    private static final int LOG_REPORT_RATE = 2; // seconds

    private final int sampleRate;

    private final int children;

    @Nonnull
    private final ReadTree readTree;

    @Nonnull
    private final ReadExternalPagedStore<DBKey, ReadTreeNode> readEPS;

    @Nonnull
    private final ConcurrentTree writeTree;

    @Nonnull
    private final AtomicBoolean terminating;

    /**
     * Fields used by the log reporting thread.
     */
    private volatile long nonLeafCount, totalCount;

    private volatile long sizeStatisticsCount, subtreeStatisticsCount;

    private volatile long prevTotalCount, prevSizeStatisticsCount, prevSubtreeStatisticsCount;

    private volatile int phase;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public TreeStatistics(@Nonnull ReadTree readTree,
            @Nonnull ConcurrentTree writeTree) {
        this(readTree, writeTree, 1, 1, new AtomicBoolean(false));
    }

    public TreeStatistics(@Nonnull ReadTree readTree,
            @Nonnull ConcurrentTree writeTree,
            int sampleRate,
            int children,
            @Nonnull AtomicBoolean terminating) {
        this.readTree = readTree;
        this.writeTree = writeTree;
        this.terminating = terminating;
        this.readEPS = readTree.getReadEps();
        this.sampleRate = sampleRate;
        this.children = children;
        scheduler.scheduleAtFixedRate(new ReporterRunnable(), 0, LOG_REPORT_RATE, TimeUnit.SECONDS);
    }

    /**
     * Read from {@link #readTree} and populate disk utilization
     * statistics into {@link #writeTree}
     */
    public void generateStatistics() {
        ReadTreeNode readRoot = readTree.getRootNode();
        ConcurrentTreeNode writeRoot = writeTree.getRootNode();
        ConcurrentTreeNode writeChildren = writeTree.getOrCreateNode(writeRoot, "children", null);
        try {
            // Generate per-node statistics
            if (!terminating.get()) {
                phase++;
                walkNodeStatistics(readRoot, writeChildren);
                log.warn("Phase (1) of (3) completed. Identified " + nonLeafCount + " non-leaf nodes " +
                         "and " + totalCount + " total nodes.");
            }

            // Generate nested subtree information
            if (!terminating.get()) {
                phase++;
                walkSubtreeStatistics(writeRoot);
                log.warn("Phase (2) of (3) completed.");
            }

            // Compute byte sizes for all subtrees
            if (!terminating.get()) {
                phase++;
                walkSizeStatistics(writeRoot);
                log.warn("Phase (3) of (3) completed.");
            }

            pageDBStatistics(writeRoot);

            // set phase to final value in case previous phases failed to run
            phase = 4;
        } finally {
            writeChildren.release();
            scheduler.shutdownNow();
        }
    }

    /**
     * Phase 1: generate statistics on a per-node basis.
     * All information this is calculated in this phase is
     * local to the current node. It does not need any
     * information from the child nodes.
     **/

    /**
     * Breadth-first search to generate per-node statistics.
     * Invokes {@link #generateNodeStatistics(com.addthis.hydra.data.tree.ReadTreeNode,
     * com.addthis.hydra.data.tree.ReadTreeNode, com.addthis.hydra.data.tree.ConcurrentTreeNode)}
     * on each node and recursively calls {@link #walkNodeStatistics(
     *com.addthis.hydra.data.tree.ReadTreeNode, com.addthis.hydra.data.tree.ConcurrentTreeNode)}.
     */
    private void walkNodeStatistics(ReadTreeNode readParent, ConcurrentTreeNode writeParent) {
        if (readParent.nodes == 0) {
            return;
        }

        nonLeafCount++;

        walkChildrenStatistics(readParent, writeParent);

        ClosableIterator<DataTreeNode> iterator = writeParent.getIterator();

        try {
            while (iterator.hasNext() && !terminating.get()) {
                ReadTreeNode readNode;
                try {
                    ConcurrentTreeNode node = (ConcurrentTreeNode) iterator.next();
                    readNode = (ReadTreeNode) readTree.getNode(readParent, node.getName());
                } catch (Exception ex) {
                    log.warn(ex.toString());
                    log.warn(printStackTrace(ex));
                    continue;
                }

                if (readNode.nodes == 0) {
                    continue;
                }

                ConcurrentTreeNode writeNode = null, writeChildren = null;

                try {
                    writeNode = writeTree.getOrCreateNode(writeParent, readNode.getName(), null);
                    writeChildren = writeTree.getOrCreateNode(writeNode, "children", null);
                    writeNode.release();
                    writeNode = null;
                    walkNodeStatistics(readNode, writeChildren);
                } finally {
                    if (writeNode != null) {
                        writeNode.release();
                    }
                    if (writeChildren != null) {
                        writeChildren.release();
                    }
                }
            }
        } finally {
            iterator.close();
        }
    }

    /**
     * Use the iterator to generate per-node statistics for all the
     * selected children. The selection of children is determined by
     * the iterator.
     */
    private void iteratingWalkChildrenStatistics(ClosableIterator<DataTreeNode> iterator,
            ReadTreeNode readParent,
            ConcurrentTreeNode writeParent) {
        try {
            while (iterator.hasNext() && !terminating.get()) {
                try {
                    ReadTreeNode readNode = (ReadTreeNode) iterator.next();
                    generateNodeStatistics(readNode, readParent, writeParent);
                } catch (Exception ex) {
                    log.warn(ex.toString());
                    log.warn(printStackTrace(ex));
                }
            }
        } finally {
            iterator.close();
        }
    }

    /**
     * Decides whether to use a sample-based iterator or a non-sample-based
     * iterator for child node selection.
     */
    private void walkChildrenStatistics(ReadTreeNode readParent, ConcurrentTreeNode writeParent) {
        int nodeCount = readParent.getNodeCount();

        ClosableIterator<DataTreeNode> iterator = null;

        try {
            if (sampleRate < 2 || nodeCount < children) {
                iterator = readParent.getIterator();
            } else {
                iterator = readParent.getIterator(sampleRate);
            }

            iteratingWalkChildrenStatistics(iterator, readParent, writeParent);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    /**
     * Generate statistics on a specific node. Ignores subtree information.
     * Invoked by {@link #walkNodeStatistics(com.addthis.hydra.data.tree.ReadTreeNode,
     * com.addthis.hydra.data.tree.ConcurrentTreeNode)}.
     *
     * @param readNode        target node to inspect
     * @param readNodeParent  parent of the target node to inspect
     * @param writeNodeParent parent of the new node that is created
     * @pre The lease has been acquired on the writeNodeParent.
     * @post The lease remains acquires on the writeNodeParent.
     */
    private void generateNodeStatistics(ReadTreeNode readNode,
            ReadTreeNode readNodeParent,
            ConcurrentTreeNode writeNodeParent) {
        totalCount++;
        ConcurrentTreeNode newChild = null, nodeState = null;
        try {
            String name = readNode.getName();
            newChild = writeTree.getOrCreateNode(writeNodeParent, name, null);

            // get key statistics
            ReadTree.CacheKey cacheKey = new ReadTree.CacheKey(readNodeParent.nodeDB(), name);
            long keyBytes = readEPS.getKeyCoder().keyEncode(cacheKey.dbkey()).length;

            // get value statistics
            CodableStatistics statistics = readEPS.getKeyCoder().valueStatistics(readNode);

            nodeState = writeTree.getOrCreateNode(newChild, "node", null);

            long attachSize = getAttachmentStatistics(statistics, nodeState);

            long valueWithAttachments = statistics.getTotalSize();
            long valueWithoutAttachments = valueWithAttachments - attachSize;
            long totalBytes = keyBytes + valueWithAttachments;

            generateNodeStatisticsEntry(nodeState, "key", keyBytes);
            generateNodeStatisticsEntry(nodeState, "valueWithAttachments", valueWithAttachments);
            generateNodeStatisticsEntry(nodeState, "valueWithoutAttachments", valueWithoutAttachments);
            generateNodeStatisticsEntry(nodeState, "total", totalBytes);

            /**
             * If this is a leaf node then set the node counter to the total size
             */
            if (readNode.nodes == 0) {
                newChild.setCounter(totalBytes);
                newChild.markChanged();
            }
        } finally {
            if (nodeState != null) {
                nodeState.release();
            }
            if (newChild != null) {
                newChild.release();
            }
        }
    }

    /**
     * Helper function to get or create a node and populate the counter of the node.
     *
     * @param parent Parent of the target node.
     * @param name   Name of the target node.
     * @param size   Value to assign to the node counter.
     */
    private void generateNodeStatisticsEntry(ConcurrentTreeNode parent, String name, long size) {
        ConcurrentTreeNode node = writeTree.getOrCreateNode(parent, name, null);
        try {
            node.setCounter(size);
            node.markChanged();
        } finally {
            node.release();
        }
    }


    /**
     * Generates data attachment statistics on a specific node.
     */
    private long getAttachmentStatistics(CodableStatistics statistics, ConcurrentTreeNode newParent) {
        long size = 0;
        Map<String, Map<Object, Long>> mapStatistics = statistics.getMapStatistics();
        ConcurrentTreeNode dataNode = writeTree.getOrCreateNode(newParent, "attachments", null);
        try {
            Map<Object, Long> attachments = mapStatistics.get("data");

            if (attachments == null) {
                return size;
            }

            for (Map.Entry<Object, Long> entry : attachments.entrySet()) {
                String name = entry.getKey().toString();
                long attachmentSize = entry.getValue();
                generateNodeStatisticsEntry(dataNode, name, attachmentSize);
                size += attachmentSize;
            }
            dataNode.setCounter(size);
            dataNode.markChanged();
            return size;
        } finally {
            dataNode.release();
        }
    }

    /**
     * Phase 2 perform a breadth-first search to collect
     * all the aggregate information about subtree.
     */

    private void walkSubtreeStatistics(ConcurrentTreeNode writeParent) {
        generateSubtreeStatistics(writeParent);

        ConcurrentTreeNode children = writeTree.getNode(writeParent, "children", true);

        ClosableIterator<DataTreeNode> iterator = null;

        try {
            if (children == null || children.nodes == 0) {
                return;
            }

            iterator = children.getIterator();

            while (iterator.hasNext() && !terminating.get()) {
                ConcurrentTreeNode writeNode = (ConcurrentTreeNode) iterator.next();
                walkSubtreeStatistics(writeNode);
            }
        } finally {
            if (children != null) {
                children.release();
            }
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    private void generateSubtreeStatistics(ConcurrentTreeNode writeNode) {
        Histogram keyHistogram = new Histogram();
        Histogram valueWithAttachments = new Histogram();
        Histogram valueWithoutAttachments = new Histogram();
        Histogram dataAttachments = new Histogram();
        Histogram total = new Histogram();
        Map<String, Histogram> attachments = new HashMap<>();

        boolean leaf = insertIntoHistograms(writeNode, keyHistogram, valueWithAttachments,
                valueWithoutAttachments, dataAttachments, total, attachments);

        if (!leaf) {
            exportFromHistograms(writeNode, keyHistogram, valueWithAttachments,
                    valueWithoutAttachments, dataAttachments, total, attachments);
        }
    }

    /**
     * Traverse the children of the target node and populate the appropriate histograms.
     * Returns true if-and-only-if the current node is a leaf node.
     */
    private boolean insertIntoHistograms(ConcurrentTreeNode target,
            Histogram keyHistogram,
            Histogram valueWithAttachments,
            Histogram valueWithoutAttachments,
            Histogram dataAttachments,
            Histogram total,
            Map<String, Histogram> attachments) {
        ConcurrentTreeNode children = writeTree.getNode(target, "children", false);

        if (children == null || children.nodes == 0) {
            return true;
        }

        ClosableIterator<DataTreeNode> iterator = children.getIterator();

        try {
            while (iterator.hasNext() && !terminating.get()) {
                subtreeStatisticsCount++;

                ConcurrentTreeNode child = (ConcurrentTreeNode) iterator.next();

                ConcurrentTreeNode nodeChild = writeTree.getNode(child, "node", true);

                if (nodeChild == null) {
                    continue;
                }

                try {
                    ConcurrentTreeNode item;

                    item = writeTree.getNode(nodeChild, "key", false);
                    keyHistogram.offer(item.getCounter());

                    item = writeTree.getNode(nodeChild, "valueWithAttachments", false);
                    valueWithAttachments.offer(item.getCounter());

                    item = writeTree.getNode(nodeChild, "valueWithoutAttachments", false);
                    valueWithoutAttachments.offer(item.getCounter());

                    item = writeTree.getNode(nodeChild, "attachments", false);
                    dataAttachments.offer(item.getCounter());

                    item = writeTree.getNode(nodeChild, "total", false);
                    total.offer(item.getCounter());

                    ConcurrentTreeNode attachmentChild = writeTree.getNode(nodeChild, "attachments", true);

                    if (attachmentChild != null) {
                        ClosableIterator<DataTreeNode> dataIterator = attachmentChild.getIterator();
                        try {
                            while (dataIterator.hasNext()) {
                                ConcurrentTreeNode dataNode = (ConcurrentTreeNode) dataIterator.next();
                                String name = dataNode.getName();
                                Histogram histogram = attachments.get(name);
                                if (histogram == null) {
                                    histogram = new Histogram();
                                    attachments.put(name, histogram);
                                }
                                histogram.offer(dataNode.getCounter());
                            }
                        } finally {
                            dataIterator.close();
                            attachmentChild.release();
                        }
                    }
                } finally {
                    nodeChild.release();
                }
            }
        } finally {
            iterator.close();
        }
        return false;
    }

    /**
     * Move data from the histograms into the output tree.
     */
    private void exportFromHistograms(ConcurrentTreeNode writeNode,
            Histogram keyHistogram,
            Histogram valueWithAttachments,
            Histogram valueWithoutAttachments,
            Histogram dataAttachments,
            Histogram total,
            Map<String, Histogram> attachmentsMap) {
        if (!terminating.get()) {

            ConcurrentTreeNode histograms = writeTree.getOrCreateNode(writeNode, "histograms", null);
            try {

                ConcurrentTreeNode children = writeTree.getOrCreateNode(histograms, "children", null);

                try {
                    updateHistograms(keyHistogram, "key", children);
                    updateHistograms(valueWithAttachments, "valueWithAttachments", children);
                    updateHistograms(valueWithoutAttachments, "valueWithoutAttachments", children);
                    updateHistograms(dataAttachments, "attachments", children);
                    updateHistograms(total, "total", children);
                } finally {
                    children.release();
                }

                ConcurrentTreeNode attachments = writeTree.getOrCreateNode(histograms, "attachments", null);

                try {
                    for (Map.Entry<String, Histogram> entry : attachmentsMap.entrySet()) {
                        updateHistograms(entry.getValue(), entry.getKey(), attachments);
                    }
                } finally {
                    attachments.release();
                }
            } finally {
                histograms.release();
            }
        }
    }

    /**
     * Create a new node in the output tree and populate it with histogram data.
     */
    private void updateHistograms(Histogram histogram, String name, ConcurrentTreeNode parent) {
        ConcurrentTreeNode node;
        node = writeTree.getOrCreateNode(parent, name, null);
        try {
            exportHistogram(histogram, node);
        } finally {
            node.release();
        }
    }

    /**
     * Phase 3: perform a depth-first search to populate the
     * total disk utilization for each nested subtree.
     */

    /**
     * Depth-first search to populate the size statistics
     *
     * @pre The lease has been acquired on writeNode
     * @post The lease remains acquired on writeNode
     */
    private void walkSizeStatistics(ConcurrentTreeNode writeNode) {
        sizeStatisticsCount++;

        ConcurrentTreeNode children = writeTree.getNode(writeNode, "children", true);

        if (children == null) {
            return;
        }

        ClosableIterator<DataTreeNode> iterator = null;

        long size = 0;
        long nodeSize = 0;

        try {
            iterator = children.getIterator();

            while (iterator.hasNext() && !terminating.get()) {
                ConcurrentTreeNode node = writeTree.getOrCreateNode(children,
                        iterator.next().getName(), null);

                try {
                    walkSizeStatistics(node);
                    size += node.getCounter();
                } finally {
                    node.release();
                }
            }
            children.setCounter(size);
            children.markChanged();
        } finally {
            if (iterator != null) {
                iterator.close();
            }

            children.release();
        }

        if (!terminating.get()) {
            ConcurrentTreeNode writeNodeChild = writeTree.getNode(writeNode, "node", true);

            if (writeNodeChild != null) {
                try {
                    ConcurrentTreeNode totalNode = writeTree.getNode(writeNodeChild, "total", false);
                    nodeSize = totalNode.getCounter();
                    writeNodeChild.setCounter(nodeSize);
                    writeNodeChild.markChanged();
                } finally {
                    writeNodeChild.release();
                }
            }

            writeNode.setCounter(size + nodeSize);
            writeNode.markChanged();
        }
    }

    private void pageDBStatistics(ConcurrentTreeNode writeRoot) {
        ExternalPagedStoreMetrics metrics = readEPS.getMetrics();

        if (metrics == null) {
            return;
        }

        com.yammer.metrics.core.Histogram pageSize = metrics.getPageSize();

        ConcurrentTreeNode pageDBNode = null, keyNode = null;

        try {
            pageDBNode = writeTree.getOrCreateNode(writeRoot, "pagedb", null);
            keyNode = writeTree.getOrCreateNode(pageDBNode, "keys", null);
            exportHistogram(pageSize, keyNode);
        } finally {
            if (pageDBNode != null) {
                pageDBNode.release();
            }
            if (keyNode != null) {
                keyNode.release();
            }
        }


    }

    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: [read tree root] [write tree root]");
            System.exit(2);
        }

        File readRoot = new File(args[0]);
        File writeRoot = new File(args[1]);

        ReadTree readTree = new ReadTree(readRoot, true);
        ConcurrentTree writeTree = new ConcurrentTree(writeRoot);

        TreeStatistics statistics = new TreeStatistics(readTree, writeTree);

        statistics.generateStatistics();

        readTree.close();
        writeTree.close();
    }

    public class ReporterRunnable implements Runnable {

        @Override
        public void run() {
            try {
                if (phase == 1) {
                    long count = totalCount;
                    double rate = (count - prevTotalCount) / LOG_REPORT_RATE;
                    String rateString = String.format("%.2f", rate);
                    log.warn("Phase (1) of (3). Traversed " + count +
                             " nodes (" + rateString + " / sec)");
                    prevTotalCount = count;
                } else if (phase == 2) {
                    long count = subtreeStatisticsCount;
                    double rate = (count - prevSubtreeStatisticsCount) / LOG_REPORT_RATE;
                    String rateString = String.format("%.2f", rate);
                    log.warn("Phase (2) of (3). Traversed " + subtreeStatisticsCount + " out of " +
                             totalCount + " total nodes (" + rateString + " / sec)");
                    prevSubtreeStatisticsCount = count;
                } else if (phase == 3) {
                    long count = sizeStatisticsCount;
                    double rate = (count - prevSizeStatisticsCount) / LOG_REPORT_RATE;
                    String rateString = String.format("%.2f", rate);
                    log.warn("Phase (3) of (3). Traversed " + sizeStatisticsCount + " out of " +
                             totalCount + " total nodes (" + rateString + " / sec)");
                    prevSizeStatisticsCount = count;
                }
            } catch (Exception ex) {
                log.warn(ex.toString());
                log.warn(printStackTrace(ex));
            }
        }
    }

    private static String printStackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Helper functions.
     */

    /**
     * Exports a histogram into the output tree.
     *
     * @pre The lease has been acquired on the parent node.
     * @post The lease remains acquired on the parent node.
     */
    private void exportHistogram(Histogram histogram, ConcurrentTreeNode node) {
        node.setCounter(histogram.getTotal());
        node.markChanged();

        generateNodeStatisticsEntry(node, "count", histogram.getCount());
        generateNodeStatisticsEntry(node, "total", histogram.getTotal());
        generateNodeStatisticsEntry(node, "max", histogram.getMax());
        generateNodeStatisticsEntry(node, "min", histogram.getMin());
        generateNodeStatisticsEntry(node, "mean", Math.round(histogram.getMean()));
        generateNodeStatisticsEntry(node, "stddev", Math.round(Math.sqrt(histogram.getVariance())));

        exportQuantile(histogram, node, 25);
        exportQuantile(histogram, node, 50);
        exportQuantile(histogram, node, 75);
        exportQuantile(histogram, node, 90);
        exportQuantile(histogram, node, 95);
        exportQuantile(histogram, node, 99);
        exportQuantile(histogram, node, 99.9);
    }

    /**
     * Exports a histogram into the output tree.
     *
     * @pre The lease has been acquired on the parent node.
     * @post The lease remains acquired on the parent node.
     */
    private void exportHistogram(com.yammer.metrics.core.Histogram histogram, ConcurrentTreeNode node) {
        node.setCounter(histogram.count());
        node.markChanged();

        generateNodeStatisticsEntry(node, "count", histogram.count());
        generateNodeStatisticsEntry(node, "total", Math.round(histogram.sum()));
        generateNodeStatisticsEntry(node, "max", Math.round(histogram.max()));
        generateNodeStatisticsEntry(node, "min", Math.round(histogram.min()));
        generateNodeStatisticsEntry(node, "mean", Math.round(histogram.mean()));
        generateNodeStatisticsEntry(node, "stddev", Math.round(histogram.stdDev()));

        exportQuantile(histogram, node, 25);
        exportQuantile(histogram, node, 50);
        exportQuantile(histogram, node, 75);
        exportQuantile(histogram, node, 90);
        exportQuantile(histogram, node, 95);
        exportQuantile(histogram, node, 99);
        exportQuantile(histogram, node, 99.9);
    }

    /**
     * Writes a quantile value from the histogram into the output tree.
     */
    private void exportQuantile(Histogram histogram, ConcurrentTreeNode parent, double quantile) {
        String name;

        if (Math.floor(quantile) == quantile) {
            name = Integer.toString((int) quantile);
        } else {
            name = Double.toString(quantile);
        }

        ConcurrentTreeNode node = writeTree.getOrCreateNode(parent, name + "%", null);

        try {
            node.setCounter(Math.round(histogram.getQuantile(quantile / 100.0)));
            node.markChanged();
        } finally {
            node.release();
        }
    }

    /**
     * Writes a quantile value from the histogram into the output tree.
     */
    private void exportQuantile(com.yammer.metrics.core.Histogram histogram,
            ConcurrentTreeNode parent, double quantile) {
        String name;

        if (Math.floor(quantile) == quantile) {
            name = Integer.toString((int) quantile);
        } else {
            name = Double.toString(quantile);
        }

        ConcurrentTreeNode node = writeTree.getOrCreateNode(parent, name + "%", null);

        try {
            Snapshot snapshot = histogram.getSnapshot();
            node.setCounter(Math.round(snapshot.getValue(quantile / 100.0)));
            node.markChanged();
        } finally {
            node.release();
        }
    }

}

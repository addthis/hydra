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
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.JitterClock;

import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.prop.DataTime;

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
 * <pre>{type : "prune",
 *  ttl : 2592000000, // 30 days
 *  relativeDown : 1},</pre>
 *
 * @user-reference
 * @hydra-name prune
 */
public class PathPrune extends PathElement {

    private static final Logger logger = LoggerFactory.getLogger(PathPrune.class);

    /**
     * Maximum age in milliseconds.
     */
    @Codec.Set(codable = true)
    private long ttl;

    /**
     * Property key name for extracting the time stamp of a node. Default is "time".
     */
    @Codec.Set(codable = true)
    private String timePropKey = "time";

    /**
     * When traversing the tree in search of the nodes to prune,
     * if this parameter is a positive integer then begin
     * the traversal this many levels lower than the
     * current location. Default is zero.
     */
    @Codec.Set(codable = true)
    private int relativeDown = 0;


    // Is it better to try to do the pruning in this method or
    // whatever is getting the TreeNodeList back?
    @Override
    public List<DataTreeNode> getNextNodeList(final TreeMapState state) {
        long now = JitterClock.globalTime();
        DataTreeNode root = state.current();

        findAndPruneChildren(root, now, relativeDown);
        return Collections.emptyList();
    }

    public void findAndPruneChildren(final DataTreeNode root, long now, int depth) {
        if (depth == 0) {
            pruneChildren(root, now);
        } else {
            ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
            while (keyNodeItr.hasNext()) {
                findAndPruneChildren(keyNodeItr.next(), now, depth - 1);
            }
            keyNodeItr.close();
        }
    }

    public void pruneChildren(final DataTreeNode root, long now) {
        ClosableIterator<DataTreeNode> keyNodeItr = root.getIterator();
        int deleted = 0;
        int kept = 0;
        int total = 0;
        while (keyNodeItr.hasNext()) {
            DataTreeNode treeNode = keyNodeItr.next();
            Map<String, TreeNodeData<?>> dataMap = treeNode.getDataMap();
            if (dataMap != null) {
                TreeNodeData timeNodeData = dataMap.get(timePropKey);
                DataTime dt = (DataTime) timeNodeData;
                if (dt != null && now - dt.last() > ttl) {
                    root.deleteNode(treeNode.getName());
                    deleted++;
                } else {
                    kept++;
                }
                total++;
                if (total % 100000 == 0) {
                    logger.info("Iterating through children of {}, deleted: {} kept: {}", new Object[]{root.getName(), deleted, kept});
                }
            }
        }
        logger.info("Iterated through children of {}, deleted: {} kept: {}", new Object[]{root.getName(), deleted, kept});
        keyNodeItr.close();
    }

}

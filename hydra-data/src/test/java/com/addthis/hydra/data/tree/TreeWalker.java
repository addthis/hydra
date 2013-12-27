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

import java.io.File;

import java.util.Iterator;

import com.addthis.basis.util.ClosableIterator;

public class TreeWalker {

    public static void main(String args[]) throws Exception {
        new TreeWalker(new File(args[0]));
    }

    public TreeWalker(File root) throws Exception {
        long mark = System.currentTimeMillis();
        Tree tree = new Tree(root, true, true);
        long openTime = System.currentTimeMillis() - mark;
        long testTime = 0;
        try {
            mark += openTime;
            TreeNode node = tree.getRootNode();
            explore(node, 0);
            testTime = System.currentTimeMillis() - mark;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            tree.close();
        }
        System.out.println("nodeCount=" + nodeCount + " hasNodes=" + hasNodes + " maxLeafs=" + maxLeafs + " maxDepth=" + maxDepth + " openTime=" + openTime + " testTime=" + testTime);
    }

    private int nodeCount;
    private int maxLeafs;
    private int hasNodes;
    private int maxDepth;

    private void explore(DataTreeNode node, int depth) throws Exception {
        nodeCount++;
        int count = 0;
        Iterator<DataTreeNode> iter = node.iterator();
        if (iter != null) {
            hasNodes++;
            try {
                while (iter.hasNext()) {
                    count++;
                    DataTreeNode next = iter.next();
                    explore(next, depth + 1);
                }
            } finally {
                if (iter instanceof ClosableIterator) {
                    ((ClosableIterator<DataTreeNode>) iter).close();
                }
            }
        }
        maxLeafs = Math.max(count, maxLeafs);
        maxDepth = Math.max(depth, maxDepth);
//		System.out.println(".. nodeCount="+nodeCount+" hasNodes="+hasNodes+" maxLeafs="+maxLeafs+" maxDepth="+maxDepth);
    }
}

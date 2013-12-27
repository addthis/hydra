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

import com.addthis.basis.util.Files;

import org.junit.Test;

public class TestTree {

    @Test
    public void test() throws Exception {
        if (System.getProperty("hydra.test.tree") == null) {
            return;
        }

        System.setProperty("hydra.tree.pagesize", "100");
        System.setProperty("hydra.tree.pagecache", "20");

        File testDir = Files.initDirectory("test-tree");
        File rnd = File.createTempFile("tree", "test", testDir);
        File rndTree = Files.initDirectory(rnd + "-tree");
        // insert
        Tree tree = new Tree(rndTree, false, true);
        TreeNode root = tree.getRootNode();
        int created = makeNodes(root, 5, 5);
        System.out.println("created " + created + " nodes");
        // close
        tree.close();
        // walk tree
        new TreeWalker(rndTree);
        // cleanup, delete
        Files.deleteDir(rndTree);
        rnd.delete();
    }

    private int makeNodes(DataTreeNode node, int descend, int count) throws Exception {
        int created = 0;
        if (descend > 0) {
            for (int i = 0; i < count; i++) {
                DataTreeNode newnode = node.getOrCreateNode(Integer.toString(i), null);
                created += makeNodes(newnode, descend - 1, count) + 1;
//				newnode.release();
            }
        }
        return created;
    }
}

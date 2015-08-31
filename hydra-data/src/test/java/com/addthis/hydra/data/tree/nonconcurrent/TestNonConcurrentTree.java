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
package com.addthis.hydra.data.tree.nonconcurrent;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.LessFiles;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeCommonParameters;
import com.addthis.hydra.data.tree.concurrent.TreeBuilder;
import com.addthis.hydra.store.db.CloseOperation;
import com.addthis.hydra.store.nonconcurrent.NonConcurrentPage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNonConcurrentTree {

    private static final Logger log = LoggerFactory.getLogger(TestNonConcurrentTree.class);

    private static final CloseOperation close = CloseOperation.TEST;

    private File makeTemporaryDirectory() throws IOException {
        final File temp;

        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return temp;
    }


    @Test
    public void getOrCreateOneThread() throws Exception {
        log.info("getOrCreateOneThread");
        File dir = makeTemporaryDirectory();
        try {
            NonConcurrentTree tree = new NonConcurrentTree(dir, 100, 100, NonConcurrentPage.NonConcurrentPageFactory.singleton);
            NonConcurrentTreeNode root = tree.getRootNode();
            for (int i = 0; i < 1000; i++) {
                NonConcurrentTreeNode node = tree.getOrCreateNode(root, Integer.toString(i), null);
                assertNotNull(node);
                assertEquals(Integer.toString(i), node.getName());
            }
            for (int i = 0; i < 1000; i++) {
                NonConcurrentTreeNode node = tree.getNode(root, Integer.toString(i), true);
                assertNotNull(node);
                assertEquals(Integer.toString(i), node.getName());
            }
            tree.close(false, close);
        } finally {
            if (dir != null) {
                LessFiles.deleteDir(dir);
            }
        }
    }

    @Test
    public void deleteOneThread() throws Exception {
        log.info("recursiveDeleteOneThread");
        File dir = makeTemporaryDirectory();
        try {
            NonConcurrentTree tree = new NonConcurrentTree(dir, 100, 100, NonConcurrentPage.NonConcurrentPageFactory.singleton);
            NonConcurrentTreeNode root = tree.getRootNode();
            NonConcurrentTreeNode parent = tree.getOrCreateNode(root, "hello", null);
            for (int i = 0; i < 100_000; i++) {
                NonConcurrentTreeNode child = tree.getOrCreateNode(parent, Integer.toString(i), null);
                assertNotNull(child);
                assertEquals(Integer.toString(i), child.getName());
                parent = child;
            }

            assertEquals(1, root.getNodeCount());

            tree.deleteNode(root, "hello");
            assertEquals(0, root.getNodeCount());
            tree.close(false, close);
        } finally {
            if (dir != null) {
                LessFiles.deleteDir(dir);
            }
        }
    }

    @Test
    public void iterateAndDeleteFast() throws Exception {
        iterateAndDelete(10);
    }

    private void iterateAndDelete(int numElements) throws Exception {
        log.info("iterateAndDelete {}", numElements);
        File dir = makeTemporaryDirectory();
        try {
            NonConcurrentTree tree = new TreeBuilder(dir).maxPageSize(5).singleThreadedTree();
            NonConcurrentTreeNode root = tree.getRootNode();

            for (int i = 0; i < numElements; i++) {
                NonConcurrentTreeNode node = tree.getOrCreateNode(root, Integer.toString(i), null);
                assertNotNull(node);
                assertEquals(Integer.toString(i), node.getName());
                NonConcurrentTreeNode child = tree.getOrCreateNode(node, Integer.toString(i), null);
                assertNotNull(child);
            }

            Random rng = new Random();

            ClosableIterator<DataTreeNode> iterator = tree.getIterator();
            try {
                int counter = 0;
                while (iterator.hasNext()) {
                    DataTreeNode node = iterator.next();
                    if (rng.nextFloat() < 0.8) {
                        String name = node.getName();
                        tree.deleteNode(root, name);
                        counter++;
                    }
                }
                log.info("Deleted " + (((float) counter) / numElements * 100.0) + " % of nodes");
            } finally {
                iterator.close();
            }

            tree.close(false, close);

        } finally {
            if (dir != null) {
                LessFiles.deleteDir(dir);
            }
        }
    }

    @Test
    public void maximumNodeIdentifier() throws Exception {
        File dir = makeTemporaryDirectory();
        try {
            NonConcurrentTree tree = new TreeBuilder(dir).singleThreadedTree();
            NonConcurrentTreeNode root = tree.getRootNode();
            for (int i = 0; i < 1000; i++) {
                NonConcurrentTreeNode node = tree.getOrCreateNode(root, Integer.toString(i), null);
                assertNotNull(node);
                assertEquals(Integer.toString(i), node.getName());
                NonConcurrentTreeNode child = tree.getOrCreateNode(node, Integer.toString(i), null);
                child.release();
                node.release();
            }
            assertTrue(tree.setNextNodeDB(Integer.MAX_VALUE));
            for (int i = 1000; i < 2000; i++) {
                NonConcurrentTreeNode node = tree.getOrCreateNode(root, Integer.toString(i), null);
                assertNotNull(node);
                assertEquals(Integer.toString(i), node.getName());
                NonConcurrentTreeNode child = tree.getOrCreateNode(node, Integer.toString(i), null);
                child.release();
                node.release();
            }
            tree.close(false, close);
        } finally {
            if (dir != null) {
                LessFiles.deleteDir(dir);
            }
        }
    }

    @Test
    public void recursiveDelete() throws Exception {
        log.info("recursiveDelete");
        File dir = makeTemporaryDirectory();
        try {
            NonConcurrentTree tree = new TreeBuilder(dir).singleThreadedTree();
            NonConcurrentTreeNode root = tree.getRootNode();
            NonConcurrentTreeNode parent = tree.getOrCreateNode(root, "0", null);
            for (int j = 0; j < TreeCommonParameters.cleanQMax; j++) {
                NonConcurrentTreeNode child = tree.getOrCreateNode(parent, Integer.toString(j), null);
                assertNotNull(child);
                assertEquals(Integer.toString(j), child.getName());
                parent.release();
                parent = child;
            }
            parent.release();

            tree.deleteNode(root, "0");

            assertEquals(0, root.getNodeCount());

            tree.close(false, close);
        } finally {
            if (dir != null) {
                LessFiles.deleteDir(dir);
            }
        }
    }


}

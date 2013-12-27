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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.text.DecimalFormat;

import com.addthis.basis.util.Files;

import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class TestTree2 {

    /**
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {
        if (System.getProperty("hydra.test.tree") == null) {
            return;
        }

        File rootDir = Files.initDirectory(new File("test.tree"));
        Files.deleteDir(rootDir);

        Tree tree = new Tree(rootDir, false, true);

        TreeNode root = tree.getRootNode();
        assertTrue(root != null);

        String nodes[] = new String[50];
        DecimalFormat fmt = new DecimalFormat("000");
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = fmt.format(i);
        }

        createTestTree(root, nodes, 0, 5);
        checkTestTree(root, nodes, 0, 5);

        // LinkedBlockingQueue<Integer> queue = new
        // LinkedBlockingQueue<Integer>(100);
        // Creator t1 = (Creator) new Creator(root, nodes, queue).init();
        // Walker t2 = (Walker) new Walker(root, nodes, queue).init();

        // t1.join();
        // t2.setDone();
        // t2.join();

        // dumpTree(root,0);
        // dumpSource();

        tree.close();

        // reopen
        tree = new Tree(rootDir, false, true);

        root = tree.getRootNode();
        assertTrue(root != null);

        checkTestTree(root, nodes, 0, 5);

        tree.close();
    }

    /**
     * @param root
     * @param nodes
     * @param start
     * @throws Exception
     */
    private void createTestTree(TreeNode root, String nodes[], int start, int end) throws Exception {
        for (int r = start; r < end; r++) {
            String rootPath[] = new String[]{"abc", "def", "ghi-" + r, "jkl", "mno"};
            DataTreeNode next = createPath(root, rootPath);
            assertTrue(next.getName().equals(checkPath(root, rootPath).getName()));
            createNodes(next, nodes);
            matchNodes(next, nodes);
            checkNodes(next, nodes);
        }
    }

    private void checkTestTree(TreeNode root, String nodes[], int start, int end) throws Exception {
        for (int r = 0; r < 5; r++) {
            String rootPath[] = new String[]{"abc", "def", "ghi-" + r, "jkl", "mno"};
            DataTreeNode next = checkPath(root, rootPath);
            matchNodes(next, nodes);
        }
    }


    private abstract class Trepanator extends Thread {

        protected TreeNode root;
        protected String[] nodes;
        protected LinkedBlockingQueue<Integer> queue;

        Trepanator(TreeNode root, String nodes[], LinkedBlockingQueue<Integer> queue) {
            this.root = root;
            this.nodes = nodes;
            this.queue = queue;
        }

        public Trepanator init() {
            start();
            return this;
        }
    }

    private class Creator extends Trepanator {

        Creator(TreeNode root, String[] nodes, LinkedBlockingQueue<Integer> queue) {
            super(root, nodes, queue);
        }

        public void run() {
            for (int i = 0; i < 1000; i++) {
                Random r = new Random();
                try {
                    int next = Math.abs(r.nextInt());
                    createTestTree(root, nodes, next, next + 5);
                    queue.put(next);
                    // System.out.println("create "+i+" = "+next);
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            System.out.println(this.getClass() + " exiting");
        }
    }

    private class Walker extends Trepanator {

        Walker(TreeNode root, String[] nodes, LinkedBlockingQueue<Integer> queue) {
            super(root, nodes, queue);
        }

        private boolean done;

        public void setDone() {
            done = true;
        }

        public void run() {
            while (true) {
                try {
                    Integer next = queue.poll(1, TimeUnit.SECONDS);
                    if (next == null) {
                        if (done) {
                            break;
                        }
                        continue;
                    }
                    checkTestTree(root, nodes, next, next + 5);
                    // System.out.println("walked "+next);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println(this.getClass() + " exiting");
        }
    }

    /**
     * @param node
     * @param child
     * @throws Exception
     */
    private int createNodes(DataTreeNode node, String child[]) throws Exception {
        // long precount = node.getNodeCount();
        int added = 0;
        for (String c : child) {
            DataTreeNode newnode = node.getNode(c);
            if (newnode == null) {
                newnode = node.getOrCreateNode(c, null);
                assertTrue(newnode != null);
                assertTrue(newnode.getName().equals(c));
                // assertTrue(++precount == node.getNodeCount());
                added++;
//				newnode.release();
            }
        }
        return added;
    }

    /**
     * @param node
     * @param path
     * @return
     * @throws Exception
     */
    private DataTreeNode createPath(DataTreeNode node, String path[]) throws Exception {
        for (String p : path) {
            DataTreeNode next = node.getOrCreateNode(p, null);
            assertTrue(next != null);
            assertTrue(next.getName().equals(p));
            node = next;
//			next.release();
        }
        return node;
    }

    /**
     * @param node
     * @param path
     * @throws Exception
     */
    private void checkNodes(DataTreeNode node, String path[]) throws Exception {
        for (String p : path) {
            assertTrue(node.getNode(p) != null);
        }
    }

    /**
     * @param node
     * @param path
     * @throws Exception
     */
    private void matchNodes(DataTreeNode node, String path[]) throws Exception {
        Iterator<DataTreeNode> iter = node.getIterator();
        for (int i = 0; i < path.length; i++) {
            assertTrue("i=" + i, iter.hasNext());
            assertTrue(iter.next().getName().equals(path[i]));
        }
        assertTrue(!iter.hasNext());
    }

    /**
     * @param node
     * @param path
     * @throws Exception
     */
    private DataTreeNode checkPath(DataTreeNode node, String path[]) throws Exception {
        for (String p : path) {
            node = node.getNode(p);
            assertTrue(node != null);
        }
        return node;
    }

    /**
     * @param root
     * @throws Exception
     */
    private void dumpTree(DataTreeNode root, int indent) throws Exception {
        Iterator<DataTreeNode> iter = root.getIterator();
        if (iter != null) {
            char ch[] = new char[indent * 2];
            Arrays.fill(ch, ' ');
            String pad = new String(ch);
            for (; iter.hasNext(); ) {
                DataTreeNode next = iter.next();
                System.out.println(pad.concat(next.getName()));
                dumpTree(next, indent + 1);
            }
        }
    }
}

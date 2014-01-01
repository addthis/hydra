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
package com.addthis.hydra.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.HashMap;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.channel.BlockingBufferedConsumer;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.Tree;
import com.addthis.hydra.data.tree.TreeCommonParameters;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeDataParent;
import com.addthis.hydra.data.tree.TreeNode;
import com.addthis.hydra.data.tree.TreeNodeData;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * test memory estimation accuracy for tree building and in queries.
 * <p/>
 * included in this test are examples of declaring and add tree node
 * data in code.
 * <p/>
 * at this time we do NOT want JUnit running this test -- it wants user
 * feedback and it does not assert any success or failure -- it's only
 * for external mem testing.
 */
@Ignore
public class TestMemEstimation {

    static {
        TreeDataParameters.registerClass("blob", BlobDataConfig.class);
        TreeNodeData.registerClass("blob", BlobData.class);
    }

    private static boolean clean = true;

    public static void main(String args[]) throws Exception {
        clean = false;
        TestMemEstimation test = new TestMemEstimation();
        test.setup();
        test.buildTree();
        test.pause();
        test.queryLiveTree();
        test.pause();
        test.queryReadOnlyTree();
        test.pause();
        test.cleanup();
    }

    public static class BlobDataConfig extends TreeDataParameters {

        @Override
        public TreeNodeData newInstance() {
            BlobData data = new BlobData();
            data.blob = new short[24];
            return data;
        }
    }

    public static class BlobData extends TreeNodeData {

        @Codec.Set(codable = true, required = true)
        private short[] blob;

        @Override
        public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, TreeDataParameters conf) {
            return true;
        }

        @Override
        public ValueObject getValue(String key) {
            return ValueFactory.create(blob.length);
        }
    }

    private HashMap<String, TreeDataParameters> nodeDataBlob;
    private Tree tree;
    private File dir;

    @Before
    public void setup() throws Exception {
        System.setProperty("eps.mem.debug", "3000");
        System.setProperty("cs.je.cacheSize", "10M");
        dir = Files.createTempDir("test-", "-memest");
        // setup tree
        TreeCommonParameters.setDefaultCleanQueueSize(5); // small clean queue
        TreeCommonParameters.setDefaultMaxCacheMem(2 * 1024 * 1024); // 2MB cache
        TreeCommonParameters.setDefaultMaxPageMem(10 * 1024); // 20K pages
        tree = new Tree(dir, false, true);
        // declare node blob data
        nodeDataBlob = new HashMap<String, TreeDataParameters>();
        nodeDataBlob.put("blob", new BlobDataConfig());
    }

    @Test
    public void buildTree() {
        final ListBundleFormat format = new ListBundleFormat();
        long mark = System.currentTimeMillis();
        TreeNode root = tree.getRootNode();
        for (int n = 0; n < 10; n++) {
            DataTreeNode b = root.getOrCreateNode("b" + n, null);
            for (int i = 0; i < 100000; i++) {
                DataTreeNode bn = b.getOrCreateNode("leaf-" + Strings.padleft(Integer.toString(i), 6, Strings.pad0), null);
                bn.updateChildData(new DataTreeNodeUpdater() {
                                       @Override
                                       public Bundle getBundle() {
                                           return new ListBundle(format);
                                       }

                                       @Override
                                       public int getCountValue() {
                                           return 1;
                                       }
                                   }, new TreeDataParent() {
                                       @Override
                                       public HashMap<String, TreeDataParameters> dataConfig() {
                                           return nodeDataBlob;
                                       }

                                       @Override
                                       public boolean countHits() {
                                           return true;
                                       }
                                   }
                );
                bn.release();
            }
            b.release();
        }
        long time = System.currentTimeMillis() - mark;
        System.out.println("built tree in " + time + " ms");
    }

    private void query(Query query, DataTree tree) throws Exception {
        long mark = System.currentTimeMillis();
        BlockingBufferedConsumer consumer = new BlockingBufferedConsumer();
        QueryOpProcessor proc = new QueryOpProcessor(consumer);
        proc.parseOps(query.getOps());
        QueryEngineSource client = new QueryEngineSourceSingle(tree);
        client.query(query, proc);
        DataTable table = consumer.getTable();
        long time = System.currentTimeMillis() - mark;
        System.out.println("scan rows = " + table.size() + " in " + time + " ms");
        for (Bundle row : table) {
            System.out.println(" --> " + row);
        }
    }

    private void queryTree() throws Exception {
        query(new Query("nojob", "+/(3)+:+hits,+mem", null), tree);
    }

    private void queryTreeScan() throws Exception {
        query(new Query("nojob", new String[] { "+/+" }, new String[] { "merge=u" }), tree);
    }

    private void pause() throws InterruptedException, IOException {
        System.out.println("** take measurements then press return **");
        new BufferedReader(new InputStreamReader(System.in)).readLine();
    }

    @Test
    public void queryLiveTree() throws Exception {
        queryTree();
        queryTreeScan();
    }

    @Test
    public void queryReadOnlyTree() throws Exception {
        tree.close();
        tree = new Tree(dir, true, false);
        queryTree();
        queryTreeScan();
    }

    @After
    public void cleanup() {
        tree.close();
        if (clean) {
            Files.deleteDir(dir);
        }
    }
}

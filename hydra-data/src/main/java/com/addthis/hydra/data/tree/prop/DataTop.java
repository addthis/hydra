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
package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.ReadNode;
import com.addthis.hydra.data.tree.ReadTreeNode;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeDataDeferredOperation;
import com.addthis.hydra.data.util.KeyTopper;

/**
 *         <p/>
 *         TODO split into 'top.hits', 'top.nodes', 'recent' and 'recent.ext'
 *         TODO have recent list fabricate TreeNodes with values
 */
public class DataTop extends TreeNodeData<DataTop.Config> implements Codec.Codable {

    /**
     * This data attachment <span class="hydra-summary">keeps a record of the top N children
     * </span>.
     * <p/>
     * <p>The top N child nodes ranked according to the number of their children can be
     * tracked by setting the parameter {@link #node} to N. The top N child nodes ranked
     * according to their hits can be tracked by setting the parameter {@link #hit} to
     * N.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "const", value : "all-urls", data : {
     *   top : {type : "top", hit : 20},
     * }},</pre>
     *
     * <p>"$" operations support the following commands in the format
     * $+{attachment}={command} :
     * <table>
     * <tr>
     * <td width="20%">hit</td>
     * <td>a string representation of the 'hit' nodes</td></tr>
     * </tr>
     * <tr>
     * <td width="20%">size</td>
     * <td>number of entries in the data attachment</td></tr>
     * </tr>
     * </table>
     *
     * <p>"%" operations support the following commands in the format /+%{attachment}={command}.
     *
     * <table>
     * <tr>
     * <td width="25%">"hit"</td>
     * <td>retrieve the child nodes stored in the hit counter of the data attachment</td></tr>
     * </tr>
     * <tr>
     * <td width="25%">"nodes"</td>
     * <td>retrieve the child nodes stored in the nodes counter of the data attachment</td></tr>
     * </tr>
     * <tr>
     * <td width="25%">"recent"</td>
     * <td>retrieve the child nodes stored in the recent counter of the data attachment</td></tr>
     * </tr>
     * </table>
     *
     * @user-reference
     * @hydra-name top
     */
    public static final class Config extends TreeDataParameters {

        /**
         * If non-zero then store the store the top N nodes ranked
         * according to the number of hits for each node.
         * Default is zero.
         */
        @Codec.Set(codable = true)
        private int hit;

        /**
         * If non-zero then store the store the top N nodes ranked
         * according to the number of children for each node.
         * Default is zero.
         */
        @Codec.Set(codable = true)
        private int node;

        /**
         * If non-zero then store the store the N most recent nodes.
         * Default is zero.
         */
        @Codec.Set(codable = true)
        private int recent;

        /**
         * If true then rank the nodes based on the number of times
         * they occur and ignore the default ranking directives. If
         * true then {@link #hit} and {@link #node} behave identically.
         * Default is false.
         */
        @Codec.Set(codable = true)
        private boolean increment;

        @Codec.Set(codable = true)
        private boolean lossy;

        @Override
        public DataTop newInstance() {
            DataTop dt = new DataTop();
            if (hit > 0) {
                dt.topHit = new KeyTopper().init().setLossy(lossy);
            }
            if (node > 0) {
                dt.topNode = new KeyTopper().init().setLossy(lossy);
            }
            if (recent > 0) {
                dt.recent = new Recent();
            }
            dt.limits = new int[]{hit, node, recent};
            return dt;
        }
    }

    @Codec.Set(codable = true)
    private KeyTopper topHit;
    @Codec.Set(codable = true)
    private KeyTopper topNode;
    @Codec.Set(codable = true)
    private Recent recent;
    @Codec.Set(codable = true, required = true)
    private int[] limits;

    private boolean increment;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        increment = conf.increment;
        return false;
    }

    @Override
    public boolean updateParentData(DataTreeNodeUpdater state, DataTreeNode parentNode,
            DataTreeNode childNode,
            List<TreeNodeDataDeferredOperation> deferredOps) {
        boolean changed = false;
        if (recent != null) {
            changed = true;
            recent.addRecent(childNode.getName(), limits[2]);
        }
        if (topNode != null) {
            changed = true;
            if (increment) {
                topNode.increment(childNode.getName(), limits[1]);
            } else {
                topNode.update(childNode.getName(), childNode.getNodeCount(), limits[1]);
            }
        }
        if (topHit != null) {
            changed = true;
            if (increment) {
                topHit.increment(childNode.getName(), limits[0]);
            } else {
                topHit.update(childNode.getName(), childNode.getCounter(), limits[0]);
            }
        }
        return changed;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key == null || key.equals("hit")) {
            return ValueFactory.create(topHit.toString());
        } else if (key.equals("size")) {
            if (topHit != null) {
                return ValueFactory.create(topHit.size());
            }
            if (topNode != null) {
                return ValueFactory.create(topNode.size());
            }
        }
        return null;
    }

    @Override
    public List<String> getNodeTypes() {
        return Arrays.asList(new String[]{"hit", "node"});
    }

    @Override
    public Collection<ReadNode> getNodes(ReadNode parent, String key) {
        if (key == null) {
            return null;
        }
        if (key.equals("recent")) {
            ArrayList<ReadNode> ret = new ArrayList<>(recent.size());
            HashSet<String> seen = new HashSet<>();
            for (String r : recent) {
                ReadNode node = parent.getNode(r);
                if (node != null && seen.add(r)) {
                    ret.add(node);
                }
            }
            return ret;
        }
        boolean byhit;
        if ((byhit = key.equals("hit")) || key.equals("node")) {
            KeyTopper map = byhit ? topHit : topNode;
            if (map == null) {
                return null;
            }
            Map.Entry<String, Long>[] top = map.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(top.length);
            for (Map.Entry<String, Long> e : top) {
                ReadNode node = parent.getNode(e.getKey());
                if (node != null) {
                    ret.add(node);
                }
            }
            return ret;
        } else if (key.equals("vhit") && topHit != null) {
            Map.Entry<String, Long>[] top = topHit.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(top.length);
            for (Map.Entry<String, Long> e : top) {
                ret.add(new VirtualTreeNode(e.getKey(), e.getValue()));
            }
            return ret;
        } else if (key.equals("phit") && topHit != null) {
            Map.Entry<String, Long>[] top = topHit.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(top.length);
            for (Map.Entry<String, Long> e : top) {
                ReadNode node = parent.getNode(e.getKey());
                if (node != null) {
                    node = ((ReadTreeNode) node).getCloneWithCount(e.getValue());
                    ret.add(node);
                }
            }
            return ret;
        } else if (key.startsWith("phitl") && topHit != null) {
            Map.Entry<String, Long>[] top = topHit.getSortedEntries();
            String dataListStr = key.substring(5);
            String[] dataListArray = dataListStr.split(",");
            // url decode
            Set<String> dataListSet = new HashSet<>();
            for (String dataElement : dataListArray) {
                dataListSet.add(Strings.urlDecode(dataElement));
            }
            ArrayList<ReadNode> ret = new ArrayList<>(top.length);
            for (Map.Entry<String, Long> e : top) {
                if (!dataListSet.contains(e.getKey())) {
                    continue;
                }
                ReadNode node = parent.getNode(e.getKey());
                if (node != null) {
                    node = ((ReadTreeNode) node).getCloneWithCount(e.getValue());
                    ret.add(node);
                }
            }
            return ret;
        }
        return null;
    }

    /** */
    public static class Recent extends LinkedList<String> {

        public Recent() {
        }

        public synchronized void addRecent(String item, int maxsize) {
            add(item);
            if (size() > maxsize) {
                removeFirst();
            }
        }
    }
}

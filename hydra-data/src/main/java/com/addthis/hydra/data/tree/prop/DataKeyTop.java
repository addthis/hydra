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
import java.util.List;
import java.util.Map.Entry;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.ReadTreeNode;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.util.ConcurrentKeyTopper;

public class DataKeyTop extends TreeNodeData<DataKeyTop.Config> implements Codec.Codable {

    /**
     * This data attachment <span class="hydra-summary">keeps a record of the top N values
     * and the number of times they're encountered</span>.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "const", value : "shard-counter"},
     * {type : "value", key : "DATE_YMD", data : {
     *   top_ips : {type : "key.top", size : 500, key : "IP"},
     * }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>"$" operations support the following commands in the format
     * $+{attachment}={command} :
     * <table>
     * <tr>
     * <td width="20%">size</td>
     * <td>number of entries in the data attachment</td></tr>
     * <tr>
     * <td>g[name]</td>
     * <td>value associated with the key "name"</td>
     * </tr>
     * <tr>
     * <td>k[number]</td>
     * <td>key associated with the i^th element. Counting is assumed to begin at 1</td>
     * </tr>
     * <tr>
     * <td>v[number]</td>
     * <td>value associated with the i^th element. Counting is assumed to begin at 1</td>
     * </tr>
     * </table>
     *
     * <p>"%" operations support the following commands in the format /+%{attachment}={command}.
     * Two consecutive equals characters ("==") are necessary to use the "hit", "node",
     * "phit", or "vhit" operations.
     *
     * <table>
     * <tr>
     * <td width="25%">"=hit" or "=node"</td>
     * <td>retrieve the child nodes that are identified by the keys stored in the data attachment</td></tr>
     * <tr>
     * <td>"=vhit"</td>
     * <td>create virtual nodes using the keys stored in the data attachment</td>
     * </tr>
     * <tr>
     * <td>"=phit"</td>
     * <td>retrieve a cloned copy of the child nodes that are identified by the keys stored in the data attachment</td>
     * </tr>
     * <tr>
     * <td>"name1:name2:name3"</td>
     * <td>create virtual nodes using the keys specified in the command</td>
     * </tr>
     * </table>
     *
     * <p>Using "%" without any arguments creates virtual nodes using the keys
     * stored in the data attachment.
     *
     * <p>Query Path Examples:</p>
     * <pre>
     *     /shard-counter/+130101$+top_ips=k1,k2,k3,k4,k5
     *     /shard-counter/+130101/+%top_ipss==hit
     * </pre>
     *
     * @user-reference
     * @hydra-name key.top
     */
    public static final class Config extends TreeDataParameters<DataKeyTop> {

        /**
         * Bundle field name from which to draw keys.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String key;

        /**
         * Bundle field name from which to draw weight (weight) values.
         */
        @Codec.Set(codable = true)
        private String weight;

        /**
         * Value filter for providing a key weight.
         */
        @Codec.Set(codable = true)
        private ValueFilter weightFilter;

        /**
         * Maximum capacity of the key topper.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private Integer size;

        /**
         * Optionally split the input with this regular expression
         * before recording the data. Default is null.
         */
        @Codec.Set(codable = true)
        private String splitRegex;

        /**
         * Optionally apply a filter before recording the data.
         * Default is null.
         */
        @Codec.Set(codable = true)
        private ValueFilter filter;

        @Override
        public DataKeyTop newInstance() {
            DataKeyTop dataKeyTop = new DataKeyTop();
            dataKeyTop.size = size;
            dataKeyTop.top = new ConcurrentKeyTopper().init(size);
            return dataKeyTop;
        }
    }

    @Codec.Set(codable = true, required = true)
    private ConcurrentKeyTopper top;
    @Codec.Set(codable = true, required = true)
    private int size;

    private BundleField keyAccess;
    private BundleField weightAccess;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, DataKeyTop.Config conf) {
        if (keyAccess == null) {
            keyAccess = state.getBundle().getFormat().getField(conf.key);
            if (conf.weight != null) {
                weightAccess = state.getBundle().getFormat().getField(conf.weight);
            }
        }
        ValueObject val = state.getBundle().getValue(keyAccess);
        if (val != null) {
            if (conf.filter != null) {
                val = conf.filter.filter(val);
                if (val == null) {
                    return false;
                }
            }
            int weightValue = 1;
            if (weightAccess != null) {
                ValueObject inc = state.getBundle().getValue(weightAccess);
                if (inc != null) {
                    weightValue = inc.asLong().getLong().intValue();
                }
            }
            if (conf.splitRegex != null) {
                // System.out.println("SPLITTING:" + val + ":" +
                // conf.splitRegex);
                String split[] = val.toString().split(conf.splitRegex);
                // System.out.println(Arrays.toString(split));
                for (int i = 0; i < split.length; i++) {
                    top.increment(split[i], weightValue, size);
                }
            } else {
                if (val.getObjectType() == ValueObject.TYPE.ARRAY) {
                    for (ValueObject obj : val.asArray()) {
                        if (conf.weightFilter != null) {
                            Double weight = conf.weightFilter.filter(obj).asDouble().getDouble();
                            if (weight != null) {
                                weightValue *= weight;
                            }
                        }
                        top.increment(obj.toString(), weightValue, size);
                    }
                } else if (val.getObjectType() == ValueObject.TYPE.MAP) {
                    for (ValueMapEntry entry : val.asMap()) {
                        int localWeight = weightValue;
                        String key = entry.getKey();
                        ValueObject weight = entry.getValue().asDouble();
                        if (weight != null) {
                            ValueObject weightDouble = conf.weightFilter.filter(weight);
                            if (weightDouble != null) {
                                localWeight *= weightDouble.asDouble().getDouble();
                            }
                        }
                        top.increment(key, localWeight, size);
                    }
                } else {
                    if (conf.weightFilter != null) {
                        Double weight = conf.weightFilter.filter(val).asDouble().getDouble();
                        if (weight != null) {
                            weightValue *= weight;
                        }
                    }
                    top.increment(val.toString(), weightValue, size);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ValueObject getValue(String key) {
        if (key != null && key.length() > 0) {
            if (key.equals("size")) {
                return ValueFactory.create(top.size());
            }
            try {
                if (key.charAt(0) == 'g') {
                    String topKey = key.substring(1);
                    Long val = top.get(topKey);
                    return ValueFactory.create(val != null ? val : 0);
                }
                if (key.charAt(0) == 'v') {
                    int pos = Integer.parseInt(key.substring(1));
                    return pos <= top.size() ? ValueFactory.create(top.getSortedEntries()[pos - 1].getValue()) : null;
                }
                if (key.charAt(0) == 'k') {
                    key = key.substring(1);
                }
                int pos = Integer.parseInt(key);
                return pos <= top.size() ? ValueFactory.create(top.getSortedEntries()[pos - 1].getKey()) : null;
            } catch (Exception e) {
                return ValueFactory.create(e.toString());
            }
        }
        return ValueFactory.create(top.toString());
    }

    /**
     * return types of synthetic nodes returned
     */
    public List<String> getNodeTypes() {
        return Arrays.asList(new String[]{"#"});
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        if (key != null && key.startsWith("=")) {
            key = key.substring(1);
            if (key.equals("hit") || key.equals("node")) {
                ConcurrentKeyTopper map = top;
                Entry<String, Long>[] top = map.getSortedEntries();
                ArrayList<DataTreeNode> ret = new ArrayList<>(top.length);
                for (Entry<String, Long> e : top) {
                    DataTreeNode node = parent.getNode(e.getKey());
                    if (node != null) {
                        ret.add(node);
                    }
                }
                return ret;
            } else if (key.equals("vhit")) {
                Entry<String, Long>[] list = top.getSortedEntries();
                ArrayList<DataTreeNode> ret = new ArrayList<>(list.length);
                for (Entry<String, Long> e : list) {
                    ret.add(new VirtualTreeNode(e.getKey(), e.getValue()));
                }
                return ret;
            } else if (key.equals("phit")) {
                Entry<String, Long>[] list = top.getSortedEntries();
                ArrayList<DataTreeNode> ret = new ArrayList<>(list.length);
                for (Entry<String, Long> e : list) {
                    DataTreeNode node = parent.getNode(e.getKey());
                    if (node != null) {
                        node = ((ReadTreeNode) node).getCloneWithCount(e.getValue());
                        ret.add(node);
                    }
                }
                return ret;
            }
        } else if (key != null) {
            String keys[] = Strings.splitArray(key, ":");
            ArrayList<DataTreeNode> list = new ArrayList<>(keys.length);
            for (String k : keys) {
                Long v = top.get(k);
                if (v != null) {
                    list.add(new VirtualTreeNode(k, v));
                }
            }
            return list.size() > 0 ? list : null;
        }
        ArrayList<DataTreeNode> list = new ArrayList<>(top.size());
        for (Entry<String, Long> s : top.getSortedEntries()) {
            list.add(new VirtualTreeNode(s.getKey(), s.getValue()));
        }
        return list;
    }
}

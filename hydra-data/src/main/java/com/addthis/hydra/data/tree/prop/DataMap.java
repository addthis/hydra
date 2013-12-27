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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

public class DataMap extends TreeNodeData<DataMap.Config> implements Codec.SuperCodable {

    /**
     * <p><span class="hydra-summary">maintains a KV map maintained as an LRU up to a given size</span>.
     * That is, new elements added to the map that would increase its size over the max are added but the element
     * of least interest based on last access time are deleted.
     * <p/>
     * <p>${attachment}={key} will return what (if anything) the map has stored for that key
     * <p/>
     * <p>%{attachment}={comma sep'd list of keys}[/+] will return all matching keys with a virtual node for each that represents
     * the stored value for that key. The /+ is what includes the virtual layer of child nodes containing the values.</p>
     *
     * @user-reference
     * @hydra-name map
     */
    public static final class Config extends TreeDataParameters<DataMap> {

        /**
         * Field to get the map key from. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String key;

        /**
         * Field to get the mapped value from. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String val;

        /**
         * Size of the map. When a new key would be placed into the map, and it would put the size over this value,
         * the oldest entry is evicted. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private Integer size;

        @Override
        public DataMap newInstance() {
            DataMap top = new DataMap();
            top.size = size;
            return top;
        }
    }

    @Override
    public void postDecode() {
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], ValueFactory.create(vals[i]));
        }
    }

    @Override
    public void preEncode() {
        keys = new String[map.size()];
        vals = new String[map.size()];
        int pos = 0;
        for (Entry<String, ValueObject> next : map) {
            keys[pos] = next.getKey();
            vals[pos++] = next.getValue().toString();
        }
    }

    @Codec.Set(codable = true, required = true)
    private String keys[];
    @Codec.Set(codable = true, required = true)
    private String vals[];
    @Codec.Set(codable = true, required = true)
    private int size;

    private BundleField keyAccess;
    private BundleField valAccess;
    private HotMap<String, ValueObject> map = new HotMap<String, ValueObject>(new HashMap());

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, DataMap.Config conf) {
        if (keyAccess == null) {
            keyAccess = state.getBundle().getFormat().getField(conf.key);
            valAccess = state.getBundle().getFormat().getField(conf.val);
        }
        ValueObject key = state.getBundle().getValue(keyAccess);
        ValueObject val = state.getBundle().getValue(valAccess);
        if (key != null) {
            synchronized (map) {
                if (val == null) {
                    map.remove(key.toString());
                } else {
                    map.put(key.toString(), val);
                    if (map.size() > size) {
                        map.removeEldest();
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ValueObject getValue(String key) {
        synchronized (map) {
            return map.get(key);
        }
    }

    /**
     * return types of synthetic nodes returned
     */
    public List<String> getNodeTypes() {
        return Arrays.asList(new String[]{"#"});
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        String keys[] = key != null ? Strings.splitArray(key, ",") : null;
        ArrayList<DataTreeNode> list = new ArrayList<>(map.size());
        synchronized (map) {
            if (keys != null && keys.length > 0) {
                for (String k : keys) {
                    ValueObject val = map.get(k);
                    if (val != null) {
                        VirtualTreeNode child = new VirtualTreeNode(val.toString(), 1);
                        VirtualTreeNode vtn = new VirtualTreeNode(k, 1, new VirtualTreeNode[]{child});
                        list.add(vtn);
                    }
                }
            } else {
                for (Entry<String, ValueObject> e : map) {
                    VirtualTreeNode child = new VirtualTreeNode(e.getValue().toString(), 1);
                    VirtualTreeNode vtn = new VirtualTreeNode(e.getKey(), 1, new VirtualTreeNode[]{child});
                    list.add(vtn);
                }
            }
        }
        return list;
    }
}

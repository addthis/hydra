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
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

import org.slf4j.Logger;


import org.slf4j.LoggerFactory;
public class DataLimitTop extends TreeNodeData<DataLimitTop.Config> implements Codec.SuperCodable {

    private static final Logger log = LoggerFactory.getLogger(DataLimitTop.class);

    /**
     * This data attachment <span class="hydra-summary">limits child nodes to top values</span>.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "const", value : "all-urls", data : {
     *   limit : {type : "limit.top", size : 5000},
     * }},</pre>
     *
     * @user-reference
     * @hydra-name limit.top
     */
    public static final class Config extends TreeDataParameters {

        /**
         * Maximum number of child nodes allowed.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private int size;

        /**
         * If true then output debugging information. Default is false.
         */
        @Codec.Set(codable = true)
        private boolean test;

        @Override
        public DataLimitTop newInstance() {
            DataLimitTop dc = new DataLimitTop();
            dc.size = size;
            dc.test = test;
            dc.top = new KeyTopper().init().setLossy(true);
            return dc;
        }
    }

    @Codec.Set(codable = true, required = true)
    private KeyTopper top;
    @Codec.Set(codable = true)
    private int size;
    @Codec.Set(codable = true)
    private boolean test;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, Config conf) {
        return false;
    }

    @Override
    public boolean updateParentData(DataTreeNodeUpdater state, DataTreeNode parentNode,
            DataTreeNode childNode,
            List<TreeNodeDataDeferredOperation> deferredOps) {
        String offer = childNode.getName();
        if (offer == null) {
            if (test) {
                log.warn("UPD null offer " + childNode);
            }
            return false;
        }
        synchronized (top) {
            // will be a nullop if the offered key does not already exist in the top
            top.incrementExisting(offer);
        }
        return true;

    }

    public static class DataLimitTopDeferredOperation extends TreeNodeDataDeferredOperation {

        final boolean test;
        final String dropped;
        final DataTreeNode parentNode;
        final DataTreeNode childNode;

        DataLimitTopDeferredOperation(boolean test, String dropped,
                DataTreeNode parentNode,
                DataTreeNode childNode) {
            this.test = test;
            this.dropped = dropped;
            this.parentNode = parentNode;
            this.childNode = childNode;
        }

        @Override
        public void run() {
            DataTreeNode probe = test ? parentNode.getNode(dropped) : null;
            if (parentNode.deleteNode(dropped)) {
                if (test) {
                    DataTreeNode find = parentNode.getNode(dropped);
                    if (find != null && find == probe) {
                        log.warn("UPD drop failure (" + parentNode + "->" + probe + ") --> " + find);
                    }
                }
            } else if (test) {
                log.warn("UPD drop failure, no such node (" + parentNode + "->" + childNode + ") on '" + dropped + "'");
            }
        }
    }

    @Override
    public boolean updateParentNewChild(DataTreeNodeUpdater state, DataTreeNode parentNode,
            DataTreeNode childNode,
            List<TreeNodeDataDeferredOperation> deferredOps) {
        String offer = childNode.getName();
        if (offer == null) {
            if (test) {
                log.warn("UPD null offer " + childNode);
            }
            return false;
        }
        String dropped;
        synchronized (top) {
            // this increment will create the node in the top if it doesn't
            // already exist with a starting count
            // equal to the minValue in the top + 1. If this put forces
            // something out of the top that value
            // will be returned so we can remove it from the tree
            dropped = top.increment(offer, size);
        }
        if (test && log.isDebugEnabled()) {
            log.debug("UPD " + parentNode + " offer=" + offer + " drop=" + dropped + " size=" + top.size());
        }
        if (dropped != null) {
            if (offer.equals(dropped)) {
                log.warn("UPD offer equals drop (" + parentNode + "->" + childNode + ") on '" + offer + "'");
                return false;
            }
            deferredOps.add(new DataLimitTopDeferredOperation(test, dropped, parentNode, childNode));
        }
        return true;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key != null && key.length() > 0) {
            if (key.equals("size")) {
                return ValueFactory.create(top.size());
            }
            try {
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
        return null;
    }

    @Override
    public Collection<ReadNode> getNodes(ReadNode parent, String key) {
        if (key == null) {
            return null;
        }
        if (key.equals("hit") || key.equals("node")) {
            KeyTopper map = top;
            Map.Entry<String, Long>[] top = map.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(top.length);
            for (Map.Entry<String, Long> e : top) {
                ReadNode node = parent.getNode(e.getKey());
                if (node != null) {
                    ret.add(node);
                }
            }
            return ret;
        } else if (key.equals("vhit")) {
            Map.Entry<String, Long>[] list = top.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(list.length);
            for (Map.Entry<String, Long> e : list) {
                ret.add(new VirtualTreeNode(e.getKey(), e.getValue()));
            }
            return ret;
        } else if (key.equals("phit")) {
            Map.Entry<String, Long>[] list = top.getSortedEntries();
            ArrayList<ReadNode> ret = new ArrayList<>(list.length);
            for (Map.Entry<String, Long> e : list) {
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

    @Override
    public void postDecode() {
        // log.warn("deserialized "+this+" with "+top.size()+" elements --> "+top);
    }

    @Override
    public void preEncode() {
        // log.warn("serializing "+this+" with "+top.size()+" elements --> "+top);
    }
}

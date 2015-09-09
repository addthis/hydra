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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public final class DataCopy extends TreeNodeData<DataCopy.Config> {

    /**
     * This data attachment <span class="hydra-summary">stores a hashmap of key values
     * captured from the current bundle</span>. You provide the list of fields to keep and
     * any value filter you want to apply and this attachment will keep a copy.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {field:"ID", data.info.copy.key:{foo:"FOO", bar:"BAR"}}
     * </pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}={label[;default value[\;]]} returns the value stored under
     * the given label or the optionally provided default value if the label is empty
     * (eg. if none of the values passed the filter). Little unsure of
     * the syntax/escaping/point of \; here.
     *
     * <p>% operations with no arguments return two levels of nodes. The first level
     * are key nodes and the second level are value nodes.</p>
     *
     * @user-reference
     */
    public static final class Config extends TreeDataParameters<DataCopy> {

        /**
         * A mapping from labels to field names. Values are taken from the field
         * names and stored under the label. This field
         * is required.
         */
        @FieldConfig(codable = true)
        private Map<String, AutoField> key;

        /**
         * Bundle fields that contain ValueMap objects to be stored in the data attachment.
         */
        @FieldConfig(codable = true)
        private AutoField[] map;

        /**
         * Mapping from field name to a constant label. Can be used to store sets of keys.
         */
        @FieldConfig(codable = true)
        private Map<String, String> set;

        /**
         * Mapping from field name to a a field name.
         */
        @FieldConfig(codable = true)
        private Map<String, AutoField> fields;

        /**
         * A mapping from labels to value filters. Before a value is stored under a label
         * if it has a value filter then the filter is applied.
         * The default is no filters.
         */
        @FieldConfig(codable = true)
        private Map<String, ValueFilter> op;

        @Override
        public DataCopy newInstance() {
            DataCopy dc = new DataCopy();
            dc.dat = new HashMap<>();
            return dc;
        }
    }

    @FieldConfig(codable = true, required = true)
    private HashMap<String, String> dat;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, DataCopy.Config conf) {
        Bundle p = state.getBundle();
        // copy values from bundle
        if (conf.key != null) {
            for (Entry<String, AutoField> entry : conf.key.entrySet()) {
                ValueObject value = entry.getValue().getValue(p);
                insertKeyValuePair(conf, p, entry.getKey(), value);
            }
        }
        if (conf.fields != null) {
            for (Entry<String, AutoField> entry : conf.fields.entrySet()) {
                ValueObject value = entry.getValue().getValue(p);
                String key = AutoField.newAutoField(entry.getKey()).getString(p).orElse(null);
                if (key != null) {
                    insertKeyValuePair(conf, p, key, value);
                }
            }
        }
        if (conf.map != null) {
           for (AutoField field : conf.map) {
               ValueObject valueObject = field.getValue(p);
               if (valueObject != null) {
                   ValueMap valueMap = valueObject.asMap();
                   for (ValueMapEntry entry : valueMap) {
                       insertKeyValuePair(conf, p, entry.getKey(), entry.getValue());
                   }
               }
           }
        }
        if (conf.set != null) {
            for (Entry<String, String> entry : conf.set.entrySet()) {
                ValueObject keyObject = AutoField.newAutoField(entry.getKey()).getValue(p);
                if (keyObject != null) {
                    ValueArray keyArray = keyObject.asArray();
                    for (ValueObject object : keyArray) {
                        insertKeyValuePair(object.toString(), entry.getValue());
                    }
                }
            }
        }
        return true;
    }

    private void insertKeyValuePair(Config conf, Bundle p, String key, ValueObject value) {
        if (conf.op != null) {
            ValueFilter fo = conf.op.get(key);
            if ((fo != null) && (value != null)) {
                value = fo.filter(value, p);
            }
        }
        if (value != null) {
            insertKeyValuePair(key, value.toString());
        }
    }

    private void insertKeyValuePair(String key, String value) {
        if (value != null) {
            dat.put(key, value);
        }
    }


    @Override
    public Collection<String> getValueTypes() {
        return dat.keySet();
    }

    @Override
    public ValueObject getValue(String key) {
        String dv = null;
        if (dat != null) {
            int ci = key.indexOf(';');
            int eci = key.indexOf("\\;");
            if ((ci > 0) && ((eci - ci) != 1)) {
                dv = key.substring(ci + 1);
                key = key.substring(0, ci);
            }
            String v = dat.get(key);
            if ((v != null) && !v.isEmpty()) {
                return ValueFactory.create(v);
            }
        }
        if (dv != null) {
            return ValueFactory.create(dv);
        } else {
            return null;
        }
    }

    /**
     * Convenience method to convert an node into an array of size one.
     */
    private static VirtualTreeNode[] generateSingletonArray(VirtualTreeNode value) {
        VirtualTreeNode[] result = new VirtualTreeNode[1];
        result[0] = value;
        return result;
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode node, String key) {
        if (dat == null) {
            return ImmutableList.of();
        }
        if (Strings.isNullOrEmpty(key)) {
            List<DataTreeNode> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : dat.entrySet()) {
                String dataKey = entry.getKey();
                String dataValue = entry.getValue();
                VirtualTreeNode child = new VirtualTreeNode(dataValue, 1);
                VirtualTreeNode parent = new VirtualTreeNode(dataKey, 1, generateSingletonArray(child));
                result.add(parent);
            }
            return result;
        } else {
            return ImmutableList.of();
        }
    }
}

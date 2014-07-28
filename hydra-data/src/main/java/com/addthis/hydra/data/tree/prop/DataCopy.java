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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;


public final class DataCopy extends TreeNodeData<DataCopy.Config> {

    /**
     * This data attachment <span class="hydra-summary">stores a hashmap of key values
     * captured from the current bundle</span>. You provide the list of fields to keep and
     * any value filter you want to apply and this attachment will keep a copy.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "value", key : "ID", data : {
     *   info : {type : "copy", key : {
     *     foo : "FOO", bar : "BAR"
     * }}}},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}={label[;default value[\;]]} returns the value stored under
     * the given label or the optionally provided default value if the label is empty
     * (eg. if none of the values passed the filter). Little unsure of
     * the syntax/escaping/point of \; here.
     *
     * <p>% operations are not supported</p>
     *
     * @user-reference
     * @hydra-name copy
     */
    public static final class Config extends TreeDataParameters<DataCopy> {

        /**
         * A mapping from labels to field names. Values are taken from the field
         * names and stored under the label. This field
         * is required.
         */
        @FieldConfig(codable = true, required = true)
        private HashMap<String, String> key;

        /**
         * A mapping from labels to value filters. Before a value is stored under a label
         * if it has a value filter then the filter is applied.
         * The default is no filters.
         */
        @FieldConfig(codable = true)
        private HashMap<String, ValueFilter> op;

        @Override
        public DataCopy newInstance() {
            DataCopy dc = new DataCopy();
            dc.dat = new HashMap<>();
            return dc;
        }
    }

    @FieldConfig(codable = true, required = true)
    private HashMap<String, String> dat;

    private HashMap<String, BundleField> keyAccess;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, DataCopy.Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = new HashMap<>();
            for (Entry<String, String> s : conf.key.entrySet()) {
                keyAccess.put(s.getKey(), p.getFormat().getField(s.getValue()));
            }
        }
        // copy values from pipeline
        for (Entry<String, BundleField> s : keyAccess.entrySet()) {
            ValueObject v = p.getValue(s.getValue());
            if (v != null) {
                if (conf.op != null) {
                    ValueFilter fo = conf.op.get(s.getKey());
                    if (fo != null) {
                        v = fo.filter(v);
                        if (v == null) {
                            continue;
                        }
                    }
                }
                dat.put(s.getKey(), v.toString());
            }
        }
        return true;
    }

    @Override
    public Collection<String> getValueTypes() {
        return dat.keySet();
    }

    @Override
    public ValueObject getValue(String key) {
        String dv = null;
        if (dat != null) {
            int ci = key.indexOf(";");
            int eci = key.indexOf("\\;");
            if (ci > 0 && eci - ci != 1) {
                dv = key.substring(ci + 1);
                key = key.substring(0, ci);
            }
            String v = dat.get(key);
            if (v != null && v.length() > 0) {
                return ValueFactory.create(v);
            }
        }
        return dv != null ? ValueFactory.create(dv) : null;
    }
}

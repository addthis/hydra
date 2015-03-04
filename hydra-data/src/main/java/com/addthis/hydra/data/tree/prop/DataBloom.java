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

import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.AbstractCustom;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeList;

import com.clearspring.analytics.stream.membership.BloomFilter;

import org.apache.commons.codec.binary.Base64;

public class DataBloom extends TreeNodeData<DataBloom.Config> implements SuperCodable {

    private static final ValueObject present = ValueFactory.create(1);

    /**
     * <p>This data attachment is a <span class="hydra-summary">bloom filter attached to a node</span>.
     * As usual, a bloom filter may return false positives.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>{type : "const", value : "service", data : {
     *   hasid : {type : "bloom", key : "ID", max : 250000},
     * }},</pre>
     * <p/>
     * <p><b>Query Path Directives</b>
     * <p/>
     * <p>${attachment}={a "~" separated list of values} returns 1 if for any value in the list the bloom filter returns 'present'.
     * Otherwise it returns null, which is handled according to the emptyOk flag.
     * <p/>
     * <p>%{attachment}={a "," separated list of values} : the list is passed through the bloom filter and only 'present' values are
     * kept. The filtered list is then matched against children of this node (the one possessing the bloom filter). All matching
     * children are returned. Note that these are actual nodes from the tree (not virtual) and thus may or may not possess
     * children of their own.</p>
     * <p/>
     * <p>The commas need to be encoded in the query UI browser in their URL-encoded form: %2c.
     * If submitting the query directly to an endpoint the commas need to be double URL-encoded:
     * %252c.</p>
     * <p/>
     * <p>Query Path Examples:</p>
     * <pre>
     *     /service$+hasid=foo~bar~bax
     *     /service/+%hasid=foo~bar~bax
     * </pre>
     *
     * @user-reference
     */
    public static final class Config extends TreeDataParameters<DataBloom> {

        /**
         * Bundle field name from which to draw bloom candidate values.
         * This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private String key;

        /**
         * Maximum number of elements under which error guarantee is expected to hold.
         * This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private int max;

        /**
         * False positive probability.
         * Default is 0.1.
         */
        @FieldConfig(codable = true)
        private double error = 0.1D;

        @Override
        public DataBloom newInstance() {
            DataBloom db = new DataBloom();
            db.filter = new BloomFilter(max, error);
            return db;
        }
    }

    @FieldConfig(codable = true)
    private byte[] raw;

    private BloomFilter filter;
    private BundleField keyAccess;

    @Override
    public ValueObject getValue(String key) {
        if (key != null) {

            String[] keys = Strings.splitArray(key, "~");
            for (String k : keys) {
                if (filter.isPresent(k)) {
                    return present;
                }
            }
        }
        return null;
    }


    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        String[] keys = Strings.splitArray(key, ",");
        TreeNodeList list = new TreeNodeList(keys.length);
        for (String k : keys) {
            if (filter.isPresent(k)) {
                DataTreeNode find = parent.getNode(k);
                if (find != null) {
                    list.add(find);
                }
            }
        }
        return list;
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        String o = ValueUtil.asNativeString(p.getValue(keyAccess));
        if (o != null) {
            filter.add(o);
            return true;
        }
        return false;
    }

    @Override
    public void postDecode() {
        filter = BloomFilter.deserialize(raw);
    }

    @Override
    public void preEncode() {
        raw = BloomFilter.serialize(filter);
    }


    public static final class FilterValue extends AbstractCustom<BloomFilter> {

        private static final String key = "BLOOM";

        public FilterValue() {
            super(null);
        }

        public FilterValue(BloomFilter bf) {
            super(bf);
        }

        @Override
        public ValueMap asMap() throws ValueTranslationException {
            ValueMap map = ValueFactory.createMap();
            map.put(key, asBytes());
            return map;
        }

        @Override
        public void setValues(ValueMap map) {
            ValueObject vo = map.get(key);
            if (vo != null) {
                BloomFilter.deserialize(vo.asBytes().asNative());
            }
        }

        @Override
        public ValueString asSimple() {
            return ValueFactory.create(Base64.encodeBase64String(asBytes().asNative()));
        }

        @Override
        public TYPE getObjectType() {
            return TYPE.CUSTOM;
        }

        @Override
        public ValueBytes asBytes() throws ValueTranslationException {
            return ValueFactory.create(BloomFilter.serialize(heldObject));
        }

        @Override
        public ValueArray asArray() throws ValueTranslationException {
            ValueArray arr = ValueFactory.createArray(1);
            arr.add(this);
            return arr;
        }

        @Override
        public ValueLong asNumeric() throws ValueTranslationException {
            return ValueFactory.create(-1L);
        }

        @Override
        public ValueLong asLong() throws ValueTranslationException {
            return ValueFactory.create(-1L);
        }

        @Override
        public ValueDouble asDouble() throws ValueTranslationException {
            return ValueFactory.create(-1D);
        }

        @Override
        public ValueString asString() throws ValueTranslationException {
            return ValueFactory.create(Base64.encodeBase64String(asBytes().asNative()));
        }
    }
}

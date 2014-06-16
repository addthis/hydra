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

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueCustom;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueSimple;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin1;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.store.util.Raw;
import com.addthis.hydra.store.util.SeenFilterBasic;

/**
 * like DataBloom but better integrated to into query. over time we need to
 * resolve this.
 */
public class DataSeen extends TreeNodeData<DataSeen.Config> implements Codec.SuperCodable {

    /**
     * <p>This data attachment is a <span class="hydra-summary">bloom filter attached to a node</span>.
     * <p/>
     * <p><b>Query Path Directives</b>
     * <p/>
     * <p>${attachment}={command} where command is one of the following:
     * <ul>
     * <li>sat : saturation of the bloom filter</li>
     * <li>bits : total number of bits allocated to the bloom filter</li>
     * <li>ck-[string] : returns 1 or 0 based on testing against bloom filter</li>
     * <li>st-[string] : returns 1 or 0 based on testing. Then insert string into filter.</li>
     * </ul>
     * <p/>
     * <p>Calling ${attachment} without any commands will return the bloom filter
     * as a value object. This will allow you to merge two bloom filters by
     * summing two bloom filter objects together.</p>
     * <p/>
     * <p>The % notation is not supported for this data attachment.</p>
     *
     * @user-reference
     * @hydra-name seen
     */
    public static final class Config extends TreeDataParameters {

        /**
         * Bundle field name from which to draw values.
         * This field is required.
         */
        @Codec.Set(codable = true)
        private String key;

        /**
         * Maximum number of elements that can be stored in the bloom filter.
         * This field is required.
         */
        @Codec.Set(codable = true)
        private int max;

        /**
         * Number of hash function evaluations for each insertion
         * operation. This parameter is usually referred to as
         * the "k" parameter in the literature. Default value is 4.
         */
        @Codec.Set(codable = true)
        private int bitsPer = 4;

        /**
         * Type of hash function that is used. The following types are available.
         * <p>0 - HASH_HASHCODE : mostly bad
         * <p>1 - HASH_HASHCODE_SHIFT_REV : mostly bad
         * <p>2 - HASH_HASHCODE_LONG_REV : mostly bad
         * <p>3 - HASH_MD5 :  marginally better accuracy, much slower
         * <p>4 - HASH_PLUGGABLE_SHIFT : best blend of speed and accuracy
         * <p>Default value is 4.
         */
        @Codec.Set(codable = true)
        private int hash = 4;

        @Override
        public DataSeen newInstance() {
            DataSeen db = new DataSeen();
            db.bloom = new SeenFilterBasic<>(max * bitsPer, bitsPer, hash);
            return db;
        }
    }

    @Codec.Set(codable = true)
    private SeenFilterBasic<Raw> bloom;

    private BundleField keyAccess;

    @Override
    public ValueObject getValue(String key) {
        if (Strings.isEmpty(key)) {
            return new ValueBloom(bloom);
        } else if (key.equals("sat")) {
            return ValueFactory.create(bloom.getSaturation());
        } else if (key.equals("bits")) {
            return ValueFactory.create(bloom.getBits());
        } else if (key.startsWith("ck-")) {
            return ValueFactory.create(bloom.getSeen(Raw.get(Bytes.urldecode(key.substring(3)))) ? 1 : 0);
        } else if (key.startsWith("st-")) {
            long set[] = bloom.getHashSet(Raw.get(Bytes.urldecode(key.substring(3))));
            boolean seen = bloom.checkHashSet(set);
            bloom.updateHashSet(set);
            return ValueFactory.create(seen ? 1 : 0);
        } else {
            return null;
        }
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        ValueObject o = p.getValue(keyAccess);
        if (o != null) {
            bloom.setSeen(Raw.get(ValueUtil.asNativeString(o)));
        }
        return true;
    }

    @Override
    public void postDecode() {
    }

    @Override
    public void preEncode() {
    }

    /**
     * for working with bloom filters
     */
    public static final class ValueBloom implements ValueCustom, ValueNumber {

        private SeenFilterBasic<?> bloom;

        public ValueBloom() {
        }

        public ValueBloom(SeenFilterBasic<?> bloom) {
            this.bloom = bloom;
        }

        @Override
        public String toString() {
            try {
                return CodecJSON.encodeString(bloom);
            } catch (Exception e) {
                return super.toString();
            }
        }

        public long toLong() {
            return bloom.getSaturation();
        }

        @Override
        public ValueNumber avg(int count) {
            return this;
        }

        @Override
        public ValueNumber diff(ValueNumber val) {
            return this;
        }

        @Override
        public ValueNumber max(ValueNumber val) {
            if (val.getClass() == getClass()) {
                ValueBloom b = (ValueBloom) val;
                return b.toLong() > toLong() ? b : this;
            }
            return this;
        }

        @Override
        public ValueNumber min(ValueNumber val) {
            if (val.getClass() == getClass()) {
                ValueBloom b = (ValueBloom) val;
                return b.toLong() < toLong() ? b : this;
            }
            return this;
        }

        @Override
        public ValueNumber sum(ValueNumber val) {
            if (val.getClass() == getClass()) {
                ValueBloom b = (ValueBloom) val;
                return new ValueBloom(b.bloom.mergeSeen(bloom));
            }
            return this;
        }

        @Override
        public TYPE getObjectType() {
            return TYPE.CUSTOM;
        }

        @Override
        public ValueBytes asBytes() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueArray asArray() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueNumber asNumber() throws ValueTranslationException {
            return this;
        }

        @Override
        public ValueLong asLong() throws ValueTranslationException {
            return ValueFactory.create(toLong());
        }

        @Override
        public ValueDouble asDouble() throws ValueTranslationException {
            return ValueFactory.create((double) toLong());
        }

        @Override
        public ValueString asString() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueCustom asCustom() throws ValueTranslationException {
            return this;
        }

        @Override
        public Class<? extends ValueCustom> getContainerClass() {
            return ValueBloom.class;
        }

        @Override
        public ValueMap asMap() throws ValueTranslationException {
            try {
                ValueMap map = ValueFactory.createMap();
                map.put("b", ValueFactory.create(CodecBin1.encodeBytes(bloom)));
                return map;
            } catch (Exception ex) {
                throw new ValueTranslationException(ex);
            }
        }

        @Override
        public void setValues(ValueMap map) {
            try {
                bloom = (SeenFilterBasic<?>) CodecBin1.decodeBytes(new SeenFilterBasic(), map.get("b").asBytes().getBytes());
            } catch (Exception ex) {
                throw new ValueTranslationException(ex);
            }
        }

        @Override
        public ValueSimple asSimple() {
            return ValueFactory.create(bloom.getSaturation());
        }
    }
}

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
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeList;
import com.clearspring.analytics.stream.quantile.TDigest;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class DataTDigest extends TreeNodeData<DataTDigest.Config> implements Codec.SuperCodable {

    /**
     * <p>This data attachment is a <span class="hydra-summary">TDigest attached to a node</span></p>
     *
     * <p>
     * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
     * <p/>
     * The special characteristics of this algorithm are:
     * <p/>
     * a) smaller summaries than Q-digest
     * <p/>
     * b) works on doubles as well as integers.
     * <p/>
     * c) provides part per million accuracy for extreme quantiles and typically <1000 ppm accuracy for middle quantiles
     * </p>
     * </p>
     *
     * <p> The {@link #key key} field is required and specifies the bundle field name from which
     * keys will be inserted into the digest.  The @{link #compression compression} field is an
     * optional parameter and specifies the trade off for acuracy vs size.  The default value
     * is 100.  The larger the value the more accurate the digest will be but larger compression
     * also results in larger object size.
     * </p>
     *
     * <p><b>Job Configuration Example</b>
     * <pre>
     *     {type:"counts", value:"service", data:{
     *         timeDigest:{type:"tdigest", key:"TIME", compression:100},
     *     }}
     * </pre>
     * </p>
     *
     * <p><b>Query Path Directives</b></p></p>
     *
     * <pre>"$" operations support the following commands in the format $+{attachment}={command}:
     *  cdf(x) : the number of values that are less than or equal to the given a cdf evaluated at x
     *  quantile(x) : the value of the digest for quantile x (x must be between 0 and 1)
     * </pre>
     *
     * @user-reference
     * @hydra-name tdigest
     */
    public static final class Config extends TreeDataParameters<DataTDigest> {

        /**
         * Bundle field name from which to insert keys into the sketch.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String key;

        /**
         * Optionally specify the compression of the digest.
         * <p/>
         * How should accuracy be traded for size?  A value of N here will give quantile errors
         * almost always less than 3/N with considerably smaller errors expected for extreme
         * quantiles.  Conversely, you should expect to track about 5 N centroids for this
         * accuracy.
         */
        @Codec.Set(codable = true)
        private int compression = 100;

        @Override
        public DataTDigest newInstance() {
            DataTDigest db = new DataTDigest();
            db.filter = new TDigest(compression);
            return db;
        }
    }

    @Codec.Set(codable = true)
    private byte[] raw;

    private TDigest filter;
    private BundleField valueAccess;

    @Override
    public ValueObject getValue(String key) {
        double quantile = .95;
        TDigestValue.OP op = TDigestValue.OP.QUANTILE;
        if (key != null && key.startsWith("quantile(") && key.endsWith(")")) {
            op = TDigestValue.OP.QUANTILE;
            quantile = Double.valueOf(key.substring(9, key.length() - 1));
        } else if (key != null && key.startsWith("cdf(") && key.endsWith(")")) {
            op = TDigestValue.OP.CDF;
            quantile = Double.valueOf(key.substring(4, key.length() - 1));
        }
        return new TDigestValue(filter, op, quantile);
    }


    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        String keys[] = Strings.splitArray(key, ",");
        TreeNodeList list = new TreeNodeList(keys.length);
        for (String k : keys) {
            double quantile = filter.quantile(Double.valueOf(k));
            list.add(new VirtualTreeNode(k, (long) quantile));
        }
        return list;
    }

    /**
     * updates the TDigest with value from the key field.  Expectation is that the value
     * is a number.
     */
    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle p = state.getBundle();
        if (valueAccess == null) {
            valueAccess = p.getFormat().getField(conf.key);
        }
        ValueNumber o = ValueUtil.asNumberOrParseDouble(p.getValue(valueAccess));
        if (o != null) {
            filter.add(o.asDouble().getDouble());
            return true;
        }
        return false;
    }

    @Override
    public void postDecode() {
        filter = TDigest.fromBytes(ByteBuffer.wrap(raw));
    }

    @Override
    public void preEncode() {
        int bound = filter.byteSize();
        if (bound > 0) {
            ByteBuffer buf = ByteBuffer.allocate(bound);
            filter.asSmallBytes(buf);
            raw = buf.array();
        }
    }

    public static final class TDigestValue implements ValueCustom, ValueNumber {

        enum OP {CDF, QUANTILE}

        ;
        private TDigest tdigest;
        private Double quantile;
        private OP op;

        /* required for codec */
        public TDigestValue() {
        }

        public TDigestValue(TDigest tdigest, OP op, Double quantile) {
            this.tdigest = tdigest;
            this.quantile = quantile;
            this.op = op;
        }

        @Override
        public Class<? extends ValueCustom> getContainerClass() {
            return TDigestValue.class;
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
        public ValueMap asMap() throws ValueTranslationException {
            ValueMap map = ValueFactory.createMap();
            int bound = tdigest.byteSize();
            ByteBuffer buf = ByteBuffer.allocate(bound);
            tdigest.asSmallBytes(buf);
            map.put("q", ValueFactory.create(quantile));
            map.put("o", ValueFactory.create(op.toString()));
            map.put("b", ValueFactory.create(buf.array()));
            return map;
        }

        @Override
        public ValueNumber asNumber() throws ValueTranslationException {
            return this;
        }

        @Override
        public ValueLong asLong() {
            return asDouble().asLong();
        }

        @Override
        public ValueDouble asDouble() {
            switch (op) {
                case CDF:
                    return ValueFactory.create(tdigest.cdf(quantile));
                case QUANTILE:
                    return ValueFactory.create(tdigest.quantile(quantile));
                default:
                    return ValueFactory.create(tdigest.quantile(quantile));
            }
        }

        @Override
        public ValueString asString() throws ValueTranslationException {
            return asDouble().asString();
        }

        @Override
        public ValueCustom asCustom() throws ValueTranslationException {
            return this;
        }

        @Override
        public void setValues(ValueMap valueMapEntries) {
            byte b[] = valueMapEntries.get("b").asBytes().getBytes();
            this.quantile = valueMapEntries.get("q").asDouble().getDouble();
            this.op = OP.valueOf(valueMapEntries.get("o").asString().toString());
            tdigest = TDigest.fromBytes(ByteBuffer.wrap(b));
        }

        @Override
        public ValueSimple asSimple() {
            return asDouble();
        }

        @Override
        public ValueNumber sum(ValueNumber valueNumber) {
            if (TDigestValue.class == valueNumber.getClass()) {
                return new TDigestValue(TDigest.merge(tdigest.compression(), Arrays.asList(((TDigestValue) valueNumber).tdigest)), op, quantile);
            }
            return asLong().sum(valueNumber.asLong());
        }

        private long toLong() {
            return asLong().getLong();
        }

        @Override
        public ValueNumber diff(ValueNumber valueNumber) {
            return sum(valueNumber).asDouble().diff(asDouble());
        }

        @Override
        public ValueNumber avg(int count) {
            return ValueFactory.create(asDouble().getDouble() / (double) count);
        }

        @Override
        public ValueNumber min(ValueNumber valueNumber) {
            return valueNumber.asDouble().getDouble() < asDouble().getDouble() ? valueNumber : this;
        }

        @Override
        public ValueNumber max(ValueNumber valueNumber) {
            return valueNumber.asDouble().getDouble() > asDouble().getDouble() ? valueNumber : this;
        }

        @Override
        public String toString() {
            return asString().toString();
        }
    }
}

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

import java.nio.ByteBuffer;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.AbstractCustom;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueSimple;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

import com.clearspring.analytics.stream.quantile.TDigest;

public class DataTDigest extends TreeNodeData<DataTDigest.Config> implements SuperCodable {

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
     *     {counts {value:"service", data.timeDigest.tdigest {key:"TIME", compression:100}}}
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
     */
    public static final class Config extends TreeDataParameters<DataTDigest> {

        /**
         * Bundle field name from which to insert keys into the sketch.
         * This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private String key;

        /**
         * Optionally specify the compression of the digest.
         * <p/>
         * How should accuracy be traded for size?  A value of N here will give quantile errors
         * almost always less than 3/N with considerably smaller errors expected for extreme
         * quantiles.  Conversely, you should expect to track about 5 N centroids for this
         * accuracy.
         */
        @FieldConfig(codable = true)
        private int compression = 100;

        @Override
        public DataTDigest newInstance() {
            DataTDigest db = new DataTDigest();
            db.filter = new TDigest(compression);
            return db;
        }
    }

    @FieldConfig(codable = true)
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
        String[] keys = LessStrings.splitArray(key, ",");
        List<DataTreeNode> list = new ArrayList<>(keys.length);
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
        Numeric o = ValueUtil.asNumberOrParseDouble(p.getValue(valueAccess));
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

    public static final class TDigestValue extends AbstractCustom<TDigest> implements Numeric {

        enum OP {CDF, QUANTILE}

        private Double quantile;
        private OP op;

        /* required for codec */
        public TDigestValue() {
            super(null);
        }

        public TDigestValue(TDigest tdigest, OP op, Double quantile) {
            super(tdigest);
            this.quantile = quantile;
            this.op = op;
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
            int bound = heldObject.byteSize();
            ByteBuffer buf = ByteBuffer.allocate(bound);
            heldObject.asSmallBytes(buf);
            map.put("q", ValueFactory.create(quantile));
            map.put("o", ValueFactory.create(op.toString()));
            map.put("b", ValueFactory.create(buf.array()));
            return map;
        }

        @Override public void setValues(ValueMap valueMapEntries) {
                byte[] b = valueMapEntries.get("b").asBytes().asNative();
                this.quantile = valueMapEntries.get("q").asDouble().getDouble();
                this.op = OP.valueOf(valueMapEntries.get("o").asString().toString());
                heldObject = TDigest.fromBytes(ByteBuffer.wrap(b));
        }

        @Override
        public Numeric asNumeric() throws ValueTranslationException {
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
                    return ValueFactory.create(heldObject.cdf(quantile));
                case QUANTILE:
                    return ValueFactory.create(heldObject.quantile(quantile));
                default:
                    return ValueFactory.create(heldObject.quantile(quantile));
            }
        }

        @Override
        public ValueString asString() throws ValueTranslationException {
            return asDouble().asString();
        }

        @Override
        public ValueSimple asSimple() {
            return asDouble();
        }

        @Override public Numeric sum(Numeric val) {
            if (TDigestValue.class == val.getClass()) {
                return new TDigestValue(TDigest.merge(heldObject.compression(),
                        Arrays.asList(this.heldObject, ((TDigestValue) val).heldObject)), op, quantile);
            }
            return asLong().sum(val.asLong());
        }

        private long toLong() {
            return asLong().getLong();
        }

        @Override public ValueDouble diff(Numeric val) {
            return sum(val).asDouble().diff(asDouble());
        }

        @Override
        public ValueDouble avg(int count) {
            return ValueFactory.create(asDouble().getDouble() / (double) count);
        }

        @Override public Numeric min(Numeric val) {
            if (val.asDouble().getDouble() < asDouble().getDouble()) {
                return val;
            } else {
                return this;
            }
        }

        @Override public Numeric max(Numeric val) {
            if (val.asDouble().getDouble() > asDouble().getDouble()) {
                return val;
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            return asString().toString();
        }
    }
}

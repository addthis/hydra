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

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueCustom;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
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

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.CountThenEstimate;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.clearspring.analytics.stream.cardinality.LogLog;

public class DataCounting extends TreeNodeData<DataCounting.Config> implements Codec.SuperCodable {

    private static final int VER_LOG = 0;
    private static final int VER_LINEAR = 1;
    private static final int VER_COUNTEST = 2;
    private static final int VER_ADAPTIVE = 3;
    private static final int VER_HYPER_LOG_LOG = 4;
    private static final int VER_COUNTEST_HLL = 5;
    private static final int VER_HLL_PLUS = 6;
    private static final int VER_COUNTEST_HLLP = 7;

    /**
     * This data attachment performs <span class="hydra-summary">cardinality estimation of a field</span>.
     * Multiple kinds of estimators are supported. Specify the field and an estimator will
     * attempt to keep track of the number of unique values that field has stored when
     * this node was visited.</p>
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "const", value : "shard-counter"},
     * {type : "value", key : "DATE_YMD", data : {
     *   ips : {type : "count", ver : "hllp", rsd : 0.01, key : "IP"},
     * }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <pre>"$" operations support the following commands in the format $+{attachment}={command}:
     *
     *   count : the cardinality estimation.
     *   class : the simple class name of the estimator.
     *   used  : if the estimator is linear then return the utilization. Otherwise an command.
     *   put(x): offer x / show x to the estimator to be counted. 1 if the estimate changed, else 0.</pre>
     *
     * <p>If no command is specified or an invalid command is specified : the estimator is returned as
     * a custom value type which usually resolves to count.
     *
     * <p>"%" operations are not supported.
     *
     * <p>Query Path Example:</p>
     * <pre>
     *     /shard-counter/+130101$+ips=count
     * </pre>
     *
     * @user-reference
     * @hydra-name count
     */
    public static final class Config extends TreeDataParameters<DataCounting> {

        /**
         * Field to count or estimate cardinalities for. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        String key;

        /**
         * <pre>
         * Which version of a counter to use:
         * ll   : log
         * lc   : linear
         * ce   : countest
         * ac   : adaptive
         * hll  : hyper loglog
         * ceh  : countest hll
         * hllp : hyper loglog plus
         * cehp : countest hllp
         *
         * Default is ac (adaptive). Failure to use a recognized type will result in errors.
         * </pre>
         */
        @Codec.Set(codable = true)
        private String ver = "ac";

        /**
         * Passed to ac, lc, and ll as k. Default is 0.
         */
        @Codec.Set(codable = true)
        private int size;

        /**
         * Used for ac, ce, and lc. Setting this to >=0 causes the size field to be ignored when applicable.
         * This is the maximum cardinality under which a one percent error rate is likely.
         */
        @Codec.Set(codable = true)
        int max = -1;

        /**
         * Used for ce. The point at which exact counting gives way to estimation. The default is 100.
         */
        @Codec.Set(codable = true)
        private int tip = 100;

        /**
         * Used for hll and ceh. The relative standard deviation for the counter -- smaller deviations
         * require more space. The default is 0.05.
         */
        @Codec.Set(codable = true)
        private double rsd = 0.05;

        /**
         * Used in hyperloglog plus.  The precision is the number of bits used when the cardinality
         * is calculated using the normal mode.
         */
        @Codec.Set(codable = true)
        private int p = 14;

        /**
         * Used in hyperloglog plus.  The sparse precision is the number of bits used when the cardinality
         * is calculated using the sparse mode.
         */
        @Codec.Set(codable = true)
        private int sp = 25;

        @Override
        public DataCounting newInstance() {
            DataCounting dc = new DataCounting();
            dc.ver = ver.equalsIgnoreCase("ll") ? VER_LOG
                                                : ver.equalsIgnoreCase("lc") ? VER_LINEAR
                                                                             : ver.equalsIgnoreCase("ce") ? VER_COUNTEST
                                                                                                          : ver.equalsIgnoreCase("ac") ? VER_ADAPTIVE
                                                                                                                                       : ver.equalsIgnoreCase("hll") ? VER_HYPER_LOG_LOG
                                                                                                                                                                     : ver.equalsIgnoreCase("ceh") ? VER_COUNTEST_HLL
                                                                                                                                                                                                   : ver.equalsIgnoreCase("hllp") ? VER_HLL_PLUS
                                                                                                                                                                                                                                  : ver.equalsIgnoreCase("cehp") ? VER_COUNTEST_HLLP
                                                                                                                                                                                                                                                                 : -1;
            switch (dc.ver) {
                case VER_ADAPTIVE:
                    dc.ic = max >= 0 ? AdaptiveCounting.Builder.obyCount(max).build() : new AdaptiveCounting(size);
                    if (dc.ic instanceof LinearCounting) {
                        dc.ver = VER_LINEAR;
                    }
                    break;
                case VER_COUNTEST:
                    try {
                        dc.ic = new CountThenEstimate(tip, AdaptiveCounting.Builder.obyCount(max));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case VER_LINEAR:
                    dc.ic = max >= 0 ? LinearCounting.Builder.onePercentError(max).build() : new LinearCounting(size);
                    break;
                case VER_LOG:
                    dc.ic = new LogLog(size);
                    break;
                case VER_HYPER_LOG_LOG:
                    dc.ic = new HyperLogLog(rsd);
                    break;
                case VER_COUNTEST_HLL:
                    try {
                        dc.ic = new CountThenEstimate(tip, new HyperLogLog.Builder(rsd));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case VER_HLL_PLUS:
                    dc.ic = new HyperLogLogPlus(p, sp);
                    break;
                case VER_COUNTEST_HLLP:
                    try {
                        dc.ic = new CountThenEstimate(tip, new HyperLogLogPlus.Builder(p, sp));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    throw new RuntimeException("expected 'll', 'lc', 'ac', or 'ce' for cardinality version (" + max + "," + size + "," + tip + "," + ver + ")");
            }
            return dc;
        }
    }

    /**
     * Max cardinality expected If merging estimators, this should be set to the
     * max cardinality expected after the merge
     */
    @Codec.Set(codable = true)
    private int ver;
    @Codec.Set(codable = true)
    private byte[] M;

    private ICardinality ic;
    private BundleField keyAccess;

    // for DataKeySieve
    void merge(DataCounting merge) {
        try {
            ic = ic.merge(merge.ic);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
    }

    // for DataKeySieve
    void offer(Object o) {
        ic.offer(o);
    }

    // for DataKeySieve
    long count() {
        return ic.cardinality();
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        updateCounter(p.getValue(keyAccess));
        return true;
    }

    private void updateCounter(ValueObject value) {
        if (value == null) {
            return;
        }
        switch (value.getObjectType()) {
            case INT:
            case FLOAT:
            case STRING:
            case BYTES:
            case CUSTOM:
                ic.offer(value.toString());
                break;
            case ARRAY:
                ValueArray arr = value.asArray();
                for (ValueObject o : arr) {
                    updateCounter(o);
                }
                break;
            case MAP:
                ValueMap map = value.asMap();
                for (ValueMapEntry o : map) {
                    updateCounter(ValueFactory.create(o.getKey()));
                }
                break;
        }
    }

    @Override
    public ValueObject getValue(String key) {
        if (key != null) {
            if (key.equals("count")) {
                return ValueFactory.create(ic.cardinality());
            } else if (key.equals("class")) {
                return ValueFactory.create(ic.getClass().getSimpleName());
            } else if (key.equals("used") && (ic instanceof LinearCounting)) {
                return ValueFactory.create(((LinearCounting) ic).getUtilization());
            } else if (key.startsWith("put(") && key.endsWith(")")) {
                return ValueFactory.create(ic.offer(key.substring(4, key.length() - 1)) ? 1 : 0);
            }
        }
        return new LCValue(ic);
    }

    @Override
    public void postDecode() {
        switch (ver) {
            case VER_ADAPTIVE:
                ic = new AdaptiveCounting(M);
                if (ic instanceof LinearCounting) {
                    ver = VER_LINEAR;
                }
                break;
            case VER_COUNTEST:
            case VER_COUNTEST_HLL:
                try {
                    ic = new CountThenEstimate(M);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            case VER_LINEAR:
                ic = new LinearCounting(M);
                break;
            case VER_LOG:
                ic = new LogLog(M);
                break;
            case VER_HYPER_LOG_LOG:
                try {
                    ic = HyperLogLog.Builder.build(M);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case VER_HLL_PLUS:
                try {
                    ic = HyperLogLogPlus.Builder.build(M);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case VER_COUNTEST_HLLP:
                try {
                    ic = new CountThenEstimate(M);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                throw new RuntimeException("unknown version : " + ver);
        }
    }

    @Override
    public void preEncode() {
        try {
            M = ic.getBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static final class LCValue implements ValueCustom, ValueNumber {

        public LCValue() {
        }

        public LCValue(ICardinality lc) {
            this.lc = lc;
        }

        private ICardinality lc;

        private long toLong() {
            return lc.cardinality();
        }

        @Override
        public ValueNumber avg(int count) {
            return ValueFactory.create(toLong() / count);
        }

        @Override
        public ValueNumber diff(ValueNumber val) {
            return sum(val).asLong().diff(asLong());
        }

        @Override
        public ValueNumber max(ValueNumber val) {
            return val.asLong().getLong() > toLong() ? val : this;
        }

        @Override
        public ValueNumber min(ValueNumber val) {
            return val.asLong().getLong() < toLong() ? val : this;
        }

        @Override
        public ValueNumber sum(ValueNumber val) {
            try {
                if (val.getClass() == LCValue.class) {
                    return new LCValue(lc.merge(((LCValue) val).lc));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return asLong().sum(val.asLong());
        }

        @Override
        public String toString() {
            return Long.toString(lc.cardinality());
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
            return ValueFactory.create(lc.cardinality());
        }

        @Override
        public ValueDouble asDouble() throws ValueTranslationException {
            return ValueFactory.create(lc.cardinality()).asDouble();
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
            return LCValue.class;
        }

        @Override
        public ValueMap asMap() throws ValueTranslationException {
            try {
                Class<?> c = lc.getClass();
                // note we don't track VER_COUNTEST_HLL differently here because
                // it gets treated the same as VER_COUNTEST
                int ver = (c == LinearCounting.class ? VER_LINEAR
                                                     : c == AdaptiveCounting.class ? VER_ADAPTIVE
                                                                                   : c == CountThenEstimate.class ? VER_COUNTEST
                                                                                                                  : c == LogLog.class ? VER_LOG
                                                                                                                                      : c == HyperLogLog.class ? VER_HYPER_LOG_LOG
                                                                                                                                                               : c == HyperLogLogPlus.class ? VER_HLL_PLUS
                                                                                                                                                                                            : -1);
                ValueMap map = ValueFactory.createMap();
                map.put("t", ValueFactory.create(ver));
                map.put("b", ValueFactory.create(lc.getBytes()));
                return map;
            } catch (Exception ex) {
                throw new ValueTranslationException(ex);
            }
        }

        @Override
        public void setValues(ValueMap map) {
            byte b[] = map.get("b").asBytes().getBytes();
            switch (map.get("t").asLong().getLong().intValue()) {
                case VER_LINEAR:
                    lc = new LinearCounting(b);
                    break;
                case VER_ADAPTIVE:
                    lc = new AdaptiveCounting(b);
                    break;
                case VER_COUNTEST:
                case VER_COUNTEST_HLL:
                    try {
                        lc = new CountThenEstimate(b);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case VER_LOG:
                    lc = new LogLog(b);
                    break;
                case VER_HYPER_LOG_LOG:
                    try {
                        lc = HyperLogLog.Builder.build(b);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case VER_HLL_PLUS:
                    try {
                        lc = HyperLogLogPlus.Builder.build(b);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    throw new RuntimeException("invalid count type : " + map.get("t"));
            }
        }

        @Override
        public ValueSimple asSimple() {
            return asLong();
        }
    }
}

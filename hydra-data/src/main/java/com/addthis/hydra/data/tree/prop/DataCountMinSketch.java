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
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeList;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import com.google.common.annotations.VisibleForTesting;

public class DataCountMinSketch extends TreeNodeData<DataCountMinSketch.Config> implements
                                                                                SuperCodable {

    /**
     * <p>This data attachment is a <span class="hydra-summary">count-min sketch attached to a node</span>.
     * <p/>
     * <p>The error is computed as a proportion of (T) the sum of the counts inserted into the data
     * structure. The error rate is T * e (2.71828) / width. For example if I expect to observe
     * a total count of 1,000,000 and my width is 100,000 bits then the error estimate is 10e (27.1828).
     * The confidence of the estimation falling within the error is 1 - e ^ -depth. The default value
     * of depth is 10. This has a confidence of 99.995%. The total numbers of bits allocated is
     * width * depth. You are responsible for selecting values of width and depth that
     * use reasonable amounts of memory and while having acceptable error bounds and confidence limits.</p>
     * <p/>
     * <p>The {@link #key key} field is required and specifies the bundle field name from
     * which keys will be inserted into the sketch. The {@link #count count} field is optional
     * and specifies the bundle field name from which non-negative integer values will be used
     * as counts for the associated keys. If the count field is missing then each key instance
     * is assumed to have a count of 1.</p>
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {type : "const", value : "service", data : {
     *   idcount : {type : "count.min.sketch", key : "ID", width : 100000},
     * }},
     * {type : "const", value : "pixelator", data : {
     *   idcount : {type : "count.min.sketch", key : "ID", count : "ID_COUNT", width : 100000},
     * }},
     * </pre>
     * <p/>
     * <p><b>Query Path Directives</b>
     * <p/>
     * <pre>"$" operations support the following commands in the format $+{attachment}={command}:
     * <p/>
     *   total : total of all the values inserted into the sketch.
     *   val(x): literal value estimation associated with key x</pre>
     * <p/>
     *
     * <p>If no command is specified or an invalid command is specified then the estimator returns
     * the total size. </p>
     *
     * <p>%{attachment}={a "~" separated list of keys} : generates a virtual node for each key.
     * The number of hits for each virtual node is equal to the count estimate in the sketch.
     * Keys with an estimate of 0 will not appear in the output.</p>
     * <p/>
     * <p>Query Path Examples:</p>
     * <pre>
     *     /service$+pubcount=foo
     *     /service/+%pubcount~foo,bar,bax
     * </pre>
     *
     * @user-reference
     * @hydra-name count.min.sketch
     */
    public static final class Config extends TreeDataParameters<DataCountMinSketch> {

        /**
         * Bundle field name from which to insert keys into the sketch.
         * This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private String key;

        /**
         * Optionally specify the depth of the sketch.
         * If 'confidence' is specified then ignore this value. Default is 10.
         */
        @FieldConfig(codable = true)
        private int depth = 10;

        /**
         * Confidence that the error tolerance is satisfied.
         * If 'confidence' is specified then ignore 'depth' parameter.
         * Expressed as a fraction.
         */
        private double confidence;

        /**
         * Width of the sketch in bits.
         * Either 'width' or 'percentage' are required.
         */
        @FieldConfig(codable = true)
        private int width;

        /**
         * Maximum error tolerated as percentage of cardinality.
         * Either 'width' or 'percentage' are required.
         */
        @FieldConfig(codable = true)
        private double percentage;

        /**
         * Optional bundle field name for the non-negative integer values
         * that are to be associated with each key. If not specified then
         * each key instance is assumed to have a count of 1.
         */
        @FieldConfig(codable = true)
        private String count;

        public void setKey(String key) {
            this.key = key;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public void setConfidence(double confidence) {
            this.confidence = confidence;
        }

        public void setWidth(int width) {
            this.width = width;
        }

        public void setPercentage(double percentage) {
            this.percentage = percentage;
        }

        public void setCount(String count) {
            this.count = count;
        }

        @Override
        public DataCountMinSketch newInstance() {
            DataCountMinSketch db = new DataCountMinSketch();
            if ((width == 0) && (percentage == 0.0)) {
                throw new IllegalArgumentException("Either 'width' or " +
                                                   "'percentage' must be specified.");
            } else if ((width > 0) && (percentage > 0.0)) {
                throw new IllegalArgumentException("Either 'width' or " +
                                                   "'percentage' must be specified.");
            } else if (confidence < 0.0 || confidence >= 1.0) {
                throw new IllegalArgumentException("'confidence' must be between 0 and 1");
            }
            int calcWidth = width;
            int calcDepth = depth;
            if (calcWidth == 0) {
                calcWidth =  (int) Math.ceil(Math.E / percentage);
            }
            if (confidence > 0.0) {
                calcDepth = (int) Math.ceil(-Math.log(1.0 - confidence));
            }
            db.sketch = new CountMinSketch(calcDepth, calcWidth, 0);
            return db;
        }
    }

    @FieldConfig(codable = true)
    private byte[] raw;

    private CountMinSketch sketch;
    private BundleField keyAccess;
    private BundleField countAccess;

    public DataCountMinSketch(){}

    public DataCountMinSketch(int depth, int width) {
        this.sketch = new CountMinSketch(depth, width, 0);
    }

    @Override
    public ValueObject getValue(String key) {
        if (key == null || key.equals("total")) {
            return ValueFactory.create(sketch.size());
        } else if (key.startsWith("val(") && key.endsWith(")")) {
            String input = key.substring(4, key.length() - 1);
            long count = sketch.estimateCount(input);
            return ValueFactory.create(count);
        } else {
            throw new IllegalArgumentException("Unexpected key argument " + key);
        }
    }


    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        if (key == null) {
            throw new IllegalArgumentException("No key arguments entered");
        }
        String[] keys = Strings.splitArray(key, "~");
        TreeNodeList list = new TreeNodeList(keys.length);
        for (String k : keys) {
            long count = sketch.estimateCount(k);
            list.add(new VirtualTreeNode(k, count));
        }
        return list;
    }

    /* updates the CountMinSketch
    *  if the key is absent, return false
    *  if count field is not specified, always increment by one
    *  if count field is specified and not present or invalid, do not update and return false
    *  otherwise increment key by the count field's value
    */
    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        if (conf.count != null && countAccess == null) {
            countAccess = p.getFormat().getField(conf.count);
        }
        String o = ValueUtil.asNativeString(p.getValue(keyAccess));
        if (o != null) {
            long myCount = 1;
            if (countAccess != null) {
                ValueObject v = p.getValue(countAccess);
                if (v != null) {
                    try {
                        myCount = v.asLong().getLong();
                    } catch (ValueTranslationException ignored) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            sketch.add(o, myCount);
            return true;
        }
        return false;
    }

    @Override
    public void postDecode() {
        sketch = CountMinSketch.deserialize(raw);
    }

    @Override
    public void preEncode() {
        raw = CountMinSketch.serialize(sketch);
    }

    public void add(String val, long count) {
        sketch.add(val, count);
    }

    long estimateCount(String item) {
        return sketch.estimateCount(item);
    }

}

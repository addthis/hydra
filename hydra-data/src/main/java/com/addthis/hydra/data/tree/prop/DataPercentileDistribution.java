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

import java.util.Arrays;
import java.util.List;

import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.util.KeyPercentileDistribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPercentileDistribution extends TreeNodeData<DataPercentileDistribution.Config>
        implements Codable {

    private static final Logger log = LoggerFactory.getLogger(DataPercentileDistribution.class);

    /**
     * This data attachment <span class="hydra-summary">maintains a percentile distribution</span>.
     * <p/>
     * <p>Data from this object will be returned when the data attachment is
     * referenced in the query path.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * { type : "const", value : "api"},
     * { type : "branch", list : [[
     *   { type : "value", name : "ymd", key : "DATE_YMD", data : {
     *     distribution : {type : "distribution", key : "LATENCY"},
     * }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}=options. options = [mean, median, max, min, stddev, snapshot, 75, 95,
     * 98, 99, 999]
     *
     * <p>"%" operations are not supported.
     *
     * <p>Query Path Examples:</p>
     * <pre>
     *     /api/130228$+distribution=snapshot
     *     /api/130228$+distribution=95
     * </pre>
     *
     * @user-reference
     * @hydra-name distribution
     */
    public static final class Config extends TreeDataParameters<DataPercentileDistribution> {

        /**
         * Name of the field to monitor. This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private String key;

        /**
         * Sample size. Default is 1024.
         */
        @FieldConfig(codable = true)
        private int sampleSize = 1024;

        /**
         * Optionally apply a filter before recording the data.
         * Default is null.
         */
        @FieldConfig(codable = true)
        private ValueFilter filter;

        @Override
        public DataPercentileDistribution newInstance() {
            DataPercentileDistribution dt = new DataPercentileDistribution();
            dt.histogram = new KeyPercentileDistribution().setSampleSize(sampleSize).init();
            dt.filter = filter;
            dt.key = key;
            return dt;
        }
    }


    @FieldConfig(codable = true)
    private String key;
    @FieldConfig(codable = true)
    private ValueFilter filter;
    @FieldConfig(codable = true)
    private KeyPercentileDistribution histogram;

    private AutoField keyAccess;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        if (keyAccess == null) {
            keyAccess = AutoField.newAutoField(conf.key);
            filter = conf.filter;
        }
        ValueObject val = keyAccess.getValue(state.getBundle());
        if (val != null) {
            if (filter != null) {
                val = filter.filter(val, state.getBundle());
                if (val == null) {
                    return false;
                }
            }
            if (val.getObjectType() == ValueObject.TYPE.ARRAY) {
                for (ValueObject obj : val.asArray()) {
                    update(obj);
                }
            } else {
                update(val);
            }
        }
        return true;
    }

    private void update(ValueObject value) {
        try {
            histogram.update(ValueUtil.asNumberOrParseLong(value, 10).asLong().getLong());
        } catch (Exception e) {
            log.warn("[DataPercentileDistribution] unable to update sample because input was not a number: " + value.asString().toString());
        }
    }

    @Override
    public ValueObject getValue(String key) {
        if (key == null || "".equals(key) || "mean".equals(key)) {
            return ValueFactory.create(histogram.mean());
        } else if (key.equals("max")) {
            return ValueFactory.create(histogram.max());
        } else if (key.equals("min")) {
            return ValueFactory.create(histogram.min());
        } else if (key.equals("stdev")) {
            return ValueFactory.create(histogram.stdDev());
        } else if (key.equals("median")) {
            return ValueFactory.create(histogram.getSnapshot().getMedian());
        } else if (key.equals("snapshot")) {
            return ValueFactory.create(Arrays.toString(histogram.getSnapshot().getValues()));
        } else if (key.equals("75")) {
            return ValueFactory.create(histogram.getSnapshot().get75thPercentile());
        } else if (key.equals("95")) {
            return ValueFactory.create(histogram.getSnapshot().get95thPercentile());
        } else if (key.equals("98")) {
            return ValueFactory.create(histogram.getSnapshot().get98thPercentile());
        } else if (key.equals("99")) {
            return ValueFactory.create(histogram.getSnapshot().get99thPercentile());
        } else if (key.equals("999")) {
            return ValueFactory.create(histogram.getSnapshot().get999thPercentile());
        } else {
            throw new UnsupportedOperationException("Unhandled key: " + key);
        }
    }

    @Override
    public List<String> getNodeTypes() {
        return Arrays.asList("max", "min", "mean", "stdev", "snapshot");
    }
}

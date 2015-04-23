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

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

public class DataSumFloat extends TreeNodeData<DataSumFloat.Config> {

    /**
     * This data attachment <span class="hydra-summary">computes the sum, mean value, and
     * number of instances for a specified floating-point field</span>.
     * <p/>
     * <p>By default null values are treated as zero values and included in the calculation
     * of the sum, mean value, and number of instances. The null values
     * can be treated as non-zero values by using the 'defaultValue' field.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * {field:"ID", data.shares.sumf.key"IS_SHARE"}
     * </pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}=sum returns the sum.
     * <p>${attachment}=count returns the number of instances.
     * <p>${attachment}=avg returns the sum divided by the number of instances.
     *
     * <p>% operations are not supported</p>
     *
     * <p>Query Path Example:</p>
     * <pre>
     *     /+$+shares=sum
     * </pre>
     *
     * @user-reference
     */

    public static final class Config extends TreeDataParameters<DataSumFloat> {

        @FieldConfig(codable = true)
        private String key;
        @FieldConfig(codable = true)
        private double defaultValue = 0;
        @FieldConfig(codable = true)
        private boolean warnOnError;

        @Override
        public DataSumFloat newInstance() {
            DataSumFloat ds = new DataSumFloat();
            return ds;
        }
    }

    @FieldConfig(codable = true)
    private double sum;
    @FieldConfig(codable = true)
    private long num;

    private BundleField keyAccess;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, DataSumFloat.Config conf) {
        if (keyAccess == null && conf.key != null) {
            keyAccess = state.getBundle().getFormat().getField(conf.key);
        }
        if (keyAccess != null) {
            ValueObject kv = state.getBundle().getValue(keyAccess);
            if (kv != null) {
                try {
                    double value = ValueUtil.asNumberOrParseDouble(kv).asDouble().getDouble();
                    if (!Double.isNaN(value)) {
                        sum += value;
                        num++;
                    }
                } catch (NumberFormatException ex) {
                    if (conf.warnOnError) {
                        System.out.println("uparseable " + kv + " in " + state.getBundle());
                    }
                }
            } else {
                sum += conf.defaultValue;
            }
        } else {
            num++;
            sum += conf.defaultValue;
        }
        return true;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key.equals("sum")) {
            return ValueFactory.create(sum);
        } else if (key.equals("avg")) {
            return ValueFactory.create(num != 0 ? sum / num : 0);
        } else if (key.equals("count")) {
            return ValueFactory.create(num);
        } else {
            return null;
        }
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class DataSum extends TreeNodeData<DataSum.Config> {

    private static final Logger log = LoggerFactory.getLogger(DataSum.class);

    /**
     * This data attachment <span class="hydra-summary">computes the sum, mean value, and
     * number of instances for a specified integer field</span>.
     * <p/>
     * <p>By default null values are treated as zero values and included in the calculation
     * of the sum, mean value, and number of instances. The null values can be ignored
     * completely by setting 'countMissing' field to false. Alternatively the null values
     * can be treated as non-zero values by using the 'values' field.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * { type : "value", key : "UID", data : {
     *   shares : {type :"sum", key : "IS_SHARE"},
     * }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}=sum returns the sum.
     * <p>${attachment}=num returns the number of instances.
     * <p>${attachment}=avg returns the sum divided by the number of instances.
     * <p>${attachment} returns the sum.
     *
     * <p>% operations are not supported</p>
     *
     * <p>Query Path Example:</p>
     * <pre>
     *     /+$+shares=sum
     * </pre>
     *
     * @user-reference
     * @hydra-name sum
     */
    public static final class Config extends TreeDataParameters<DataSum> {

        /**
         * The target field for calculating statistics.
         */
        @FieldConfig(codable = true)
        private String key;

        /**
         * The radix of the values stored in the key field. If this value is 0 then
         * use the String length of the target field. Default is 10.
         */
        @FieldConfig(codable = true)
        private int base = 10;

        /**
         * If countMissing is true then use this stand-in value when there is a null
         * stored in the target field. Default is 0.
         */
        @FieldConfig(codable = true)
        private int value = 0;

        /**
         * If true then use the 'value' field when there is a null in the target field. Default is true.
         */
        @FieldConfig(codable = true)
        private boolean countMissing = true;

        @Override
        public DataSum newInstance() {
            DataSum ds = new DataSum();
            return ds;
        }
    }

    @FieldConfig(codable = true)
    private long sum;
    @FieldConfig(codable = true)
    private long num;

    private BundleField keyField;

    private void sum(ValueObject vo, int value, int base, boolean countMissing) {
        if (countMissing && vo == null) {
            sum += value;
            num++;
            return;
        } else if (vo == null) {
            return;
        }
        if (vo.getObjectType() == ValueObject.TYPE.ARRAY) {
            for (ValueObject v : vo.asArray()) {
                sum(v, value, base, countMissing);
            }
            return;
        }
        if (base > 0) {
            try {
                sum += ValueUtil.asNumberOrParseLong(vo, base).asLong().getLong();
            } catch (NumberFormatException nfe) {
                log.warn("Error adding data summing. " + vo + " is not a number");
            }
        } else {
            sum += vo.toString().length();
        }
        num++;
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, DataSum.Config conf) {
        if (conf.key == null) {
            num++;
            sum += conf.value;
            return true;
        }
        if (keyField == null) {
            keyField = state.getBundle().getFormat().getField(conf.key);
        }
        sum(state.getBundle().getValue(keyField), conf.value, conf.base, conf.countMissing);
        return true;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key == null) {
            key = "sum";
        }
        switch (key) {
            case "sum":
                return ValueFactory.create(sum);
            case "avg":
                return ValueFactory.create(num > 0 ? sum / num : 0);
            case "num":
            case "count":
                return ValueFactory.create(num);
            default:
                return ValueFactory.create(sum);
        }
    }
}

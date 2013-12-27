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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

public class DataBits extends TreeNodeData<DataBits.Config> {

    /**
     * <p>This data attachment <span class="hydra-summary">counts data frequency by individual bits</span>.
     * Examines every bundle in the field set by key. The value is parsed as a long in the radix specified.
     * The mask is applied to the resulting long, and then for every bit left, we increment our total bit bucket
     * for that spot by one.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     *  // hexadecimal bit counter
     *  {type : "const", value : "bitcounter", data : {
     *      bits : {type : "bits", key : "UID", bits : 128, radix : 16}
     *  }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}={index} returns the bit bucket value for the bit index provided.
     *
     * <p>% operations are not supported</p>
     *
     * <p>Query Path Example:</p>
     * <pre>
     *     /bitcounter$+bits=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
     * </pre>
     *
     * @user-reference
     * @hydra-name bits
     */
    public static final class Config extends TreeDataParameters<DataBits> {

        /**
         * Number of bits to track starting from the least significant.
         * This field is required.
         */
        @Codec.Set(codable = true)
        private int bits;

        /**
         * Radix to parse the value in. Use 10 for numbers,
         * 36 to include all letters. This field is required..
         */
        @Codec.Set(codable = true)
        private int radix;

        /**
         * Mask to apply when incrementing bit counters. For example,
         * a mask of "1" would only count the least significant bit.
         * The default value of 0 is treated as special case in which no mask is applied.
         */
        @Codec.Set(codable = true)
        private long mask;

        /**
         * Name of bundle field to select for bit counting.
         * This field is required..
         */
        @Codec.Set(codable = true)
        private String key;

        @Override
        public DataBits newInstance() {
            DataBits db = new DataBits();
            db.bits = new long[bits];
            return db;
        }
    }

    @Codec.Set(codable = true)
    private long bits[];

    private BundleField keyAccess;

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, Config conf) {
        long val = 0;
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        ValueObject vo = p.getValue(keyAccess);
        if (!ValueUtil.isEmpty(vo)) {
            try {
                val = Long.parseLong(vo.toString(), conf.radix);
            } catch (NumberFormatException ne) {
                /* track this at some point -- who is generating? */
            }
        }
        if (val > 0) {
            long bit = 1;
            final int bl = bits.length;
            for (int i = 0; i < bl; i++) {
                if ((conf.mask == 0 || (conf.mask & bit) == bit) && (val & bit) == bit) {
                    bits[i]++;
                }
                bit <<= 1;
            }
        }
        return true;
    }

    @Override
    public ValueObject getValue(String key) {
        return ValueFactory.create(bits[Integer.parseInt(key)]);
    }
}

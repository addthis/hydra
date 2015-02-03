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

package com.addthis.hydra.data.query.op.merge;

import java.util.ArrayList;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.util.KeyTopper;

public class MergeConfig {

    public MergedValue mergeCountValue;
    public KeyTopper topper;
    public int numericArg;
    public int topColumn = -1;

    public final MergedValue[] conf;

    public MergeConfig(CharSequence args) {
        ArrayList<MergedValue> conf = new ArrayList<>(args.length());

        boolean mergeCount = false;
        for (int i = 0; i < args.length(); i++) {
            char ch = args.charAt(i);
            boolean isnum = (ch >= '0' && ch <= '9');
            if (isnum) {
                numericArg *= 10;
                numericArg += (ch - '0');
                continue;
            }
            MergedValue op = null;
            switch (ch) {
                case ',':
                    continue;
                    // next col is top
                case 't':
                    topColumn = conf.size();
                    topper = new KeyTopper().init();
                    continue;
                    // average
                case 'a':
                    op = new AverageValue();
                    break;
                // diff/subtract value
                case 'd':
                    op = new DiffValue();
                    break;
                // ignore/drop
                case 'i':
//                  op = MergeOpEnums.IGNORE;
//                  conf.add(null);
//                  continue;
                    break;
                case 'j':
                    op = new JoinedValue();
                    break;
                // last value
                case 'l':
                    op = new LastValue();
                    break;
                // part of compound key
                case 'k':
                    op = new KeyValue();
                    break;
                // max value
                case 'M':
                    op = new MaxValue();
                    break;
                // min value
                case 'm':
                    op = new MinValue();
                    break;
                // sum
                case 's':
                    op = new SumValue();
                    break;
                // cardinality
                case 'c':
                    op = new CardinalityValue();
                    break;
                // add merged row count
                case 'u':
                    mergeCount = true;
                    continue;
            }
            conf.add(op);
        }
        if (mergeCount) {
            mergeCountValue = new NumMergesValue();
            conf.add(mergeCountValue);
        }
        this.conf = conf.toArray(new MergedValue[conf.size()]);
    }

    public String handleBindAndGetKey(Bundle row, ListBundleFormat format) {
        String key = "";
        int i = 0;
        /* compute key for new line and fill from/to if not set */
        for (BundleField field : row.getFormat()) {
            if (i >= conf.length) {
                break;
            }
            MergedValue mc = conf[i++];
            if (mc == null) {
                continue;
            }
            if (mc.getFrom() == null) {
                mc.setFrom(field);
                // TODO only clone field name for non-int names, otherwise create 'next' column # as name
                mc.setTo(format.getField(field.getName()));
            }
            if (mc.isKey()) {
                ValueObject lval = row.getValue(field);
                key = key.concat(lval == null ? "" : lval.toString());
            }
        }
        if (mergeCountValue != null) {
            if (mergeCountValue.getTo() == null) {
                mergeCountValue.setTo(format.createNewField("merge_"));
            }
        }
        return key;
    }

}

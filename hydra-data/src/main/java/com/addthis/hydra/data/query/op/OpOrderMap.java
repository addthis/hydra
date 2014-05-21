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
package com.addthis.hydra.data.query.op;

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">conditionally copies values</span>.
 * <p/>
 * <p>The syntax for this operation is 'ordermap=[tuple1]:[tuple2]:etc where each tuple
 * is a sequence of four comma-separated values [testcolumn],[teststring],[from],[to].
 * This operation is applied one row at a time. For each tuple that is specified,
 * take the value at column number [testcolumn] and perform string equality to the
 * value [teststring]. If the values are equal then copy the value at column [from] into
 * column [to].
 * <p/>
 * <p>Example:</p>
 * <pre>
 * A 1 art
 * B 2 bot
 * C 3 cog
 * D 4 din
 *
 * ordermap=0,C,2,1
 *
 * A 1   art
 * B 2   bot
 * C cog cog
 * D 4   din
 * </pre>
 *
 * @user-reference
 * @hydra-name ordermap
 */
public class OpOrderMap extends AbstractRowOp {

    private final ListBundleFormat format = new ListBundleFormat();
    private final List<OrderTuple> oTuples;

    public OpOrderMap(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        oTuples = new ArrayList<>();
        try {
            String[] tuples = Strings.splitArray(args, ":");
            for (String tuple : tuples) {
                String[] arg = Strings.splitArray(tuple, ",");
                oTuples.add(new OrderTuple(arg[0], arg[1], arg[2], arg[3]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        BundleColumnBinder binder = new BundleColumnBinder(row);
        for (OrderTuple tuple : oTuples) {
            String key = ValueUtil.asNativeString(binder.getColumn(row,
                    Integer.valueOf(tuple.testColumn)));

            if (tuple.testString.equals(key)) {
                binder.setColumn(row, Integer.valueOf(tuple.to),
                        binder.getColumn(row, Integer.valueOf(tuple.from)));
            }
        }
        return row;
    }

    private static class OrderTuple {

        public String testColumn;
        public String testString;
        public String from;
        public String to;

        public OrderTuple(String testColumn, String testString, String from, String to) {
            this.testColumn = testColumn;
            this.testString = testString;
            this.from = from;
            this.to = to;
        }
    }
}

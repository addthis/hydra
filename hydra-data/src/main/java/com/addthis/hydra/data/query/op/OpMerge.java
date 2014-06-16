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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.op.merge.MergeConfig;
import com.addthis.hydra.data.query.op.merge.MergedValue;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">merges adjacent rows</span>.
 * <p/>
 * <p>The syntax for this operation is "merge=[column parameters] where
 * column parameters is a sequence of one or more of the following letters:
 * <ul>
 * <li>k - this column is a key column.</li>
 * <li>i - this column is ignored and dropped from the output.</li>
 * <li>a - generate average values for this column</li>
 * <li>d - generate iterated diff values for this column</li>
 * <li>m - generate min values for this column</li>
 * <li>M - generate max values for this column</li>
 * <li>s - generate sum values for this column</li>
 * <li>l - keep last value for this column</li>
 * <li>j - append all values for this column using "," as a separator</li>
 * <li>p - packs repeating values (not sure this does anything)</li>
 * </ul>
 * <p/>
 * <p>Key columns are specified using the "k" parameter. If two or more columns are
 * specified then the resulting keys will
 * be the <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a> of
 * the specified values. For non-key columns any rows that are merged apply the
 * column parameter operation to the values that are merged. A "u" character can be included
 * at the end of the column parameters to append a column that includes the number of merged
 * rows.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * 0 A 3
 * 1 A 1
 * 1 B 2
 * 0 A 5
 *
 * merge=iks
 *
 * A 4
 * B 2
 * A 5
 * </pre>
 *
 * @user-reference
 * @hydra-name merge
 */
public class OpMerge extends AbstractQueryOp {

    private final MergeConfig mergeConfig;
    private final int countdown;
    private final MergedValue[] conf;
    private final ListBundleFormat format = new ListBundleFormat();
    private final ChannelProgressivePromise queryPromise;

    public OpMerge(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.queryPromise = queryPromise;

        mergeConfig = new MergeConfig(args);
        countdown = mergeConfig.numericArg;
        conf = mergeConfig.conf;
    }

    String lastkey = null;
    MergedRow lastRow = null;
    int rows = 0;

    @Override
    public void send(Bundle bundle) {
        if (queryPromise.isDone()) {
            return;
        }
        String key = mergeConfig.handleBindAndGetKey(bundle, format);

        if (key.equals(lastkey)) {
            lastRow.merge(bundle);
        } else {
            maybeSendLastRow();
            lastRow = new MergedRow(conf, new ListBundle(format));
            lastRow.merge(bundle);
            lastkey = key;
        }

        if (countdown > 0 && lastRow.getMergedCount() >= countdown) {
            maybeSendLastRow();
        }
    }

    private boolean maybeSendLastRow() {
        if (lastRow != null) {
            getNext().send(lastRow.emit());
            lastRow = null;
            lastkey = null;
            return true;
        }
        return false;
    }

    @Override
    public void sendComplete() {
        QueryOp next = getNext();
        if (!queryPromise.isDone()) {
            maybeSendLastRow();
        }
        next.sendComplete();
    }
}

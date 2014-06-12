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

import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">selects a subset of the rows produced</span>.
 * <p/>
 * <p>There are two forms of this operation. "range=N" skips over the first N rows.
 * "range=N,M" skips over the first N rows and includes the range of rows until
 * row number M is reached. If M is a negative number then the rows are counted
 * from the end of the table. range is an expensive operation. Use {@link OpLimit limit}
 * whenever possible in preference over this operation.</p>
 *
 * @user-reference
 * @hydra-name range
 */
public class OpRange extends AbstractTableOp {

    private int begin;
    private int end;

    public OpRange(DataTableFactory processor, String args, ChannelProgressivePromise queryPromise) {
        super(processor, queryPromise);
        int[] ov = csvToInts(args);
        begin = ov[0];
        end = ov.length > 1 ? ov[1] : 0;
    }

    @Override
    public DataTable tableOp(DataTable result) {
        if (begin < 0) {
            begin = result.size() + Math.max(begin, -result.size());
        }
        if (end <= 0) {
            end = result.size() + end;
        }
        if (end < begin) {
            int tmp = end;
            end = begin;
            begin = tmp;
        }
        DataTable qr = createTable(end - begin);
        int resultSize = result.size();
        for (int i = begin; i < Math.min(end, resultSize); i++) {
            if (i < resultSize) {
                qr.append(result.get(i));
            }
        }
        return qr;
    }
}

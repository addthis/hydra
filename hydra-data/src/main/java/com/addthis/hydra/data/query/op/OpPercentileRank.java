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
import java.util.Collections;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryStatusObserver;


/**
 * <p>This query operation <span class="hydra-summary">calculates the percentile rank of a column</span>.
 * <p>The syntax for this operation is percentrank=N:M. N is the input column and M is the output column.
 * <p>Example:</p>
 * <pre>a 8
 * b 1
 * c 7
 * d 2
 * <p/>
 * apply percentrank=1:2
 * <p/>
 * a 8 1.0
 * b 1 .25
 * c 7 .75
 * d 2 .5</pre>
 *
 * @user-reference
 * @hydra-name percentrank
 */
public class OpPercentileRank extends AbstractTableOp {

    private int inputCol;
    private int outputCol;

    public OpPercentileRank(DataTableFactory tableFactory, String args, QueryStatusObserver queryStatusObserver) {
        super(tableFactory, queryStatusObserver);
        try {
            String opt[] = args.split(":");
            if (opt.length == 2) {
                inputCol = Integer.parseInt(opt[0]);
                outputCol = Integer.parseInt(opt[1]);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public DataTable tableOp(DataTable result) {
        BundleColumnBinder binder = getSourceColumnBinder(result);
        List<Long> colValues = new ArrayList<>(result.size());
        for (Bundle row : result) {
            try {
                long val = binder.getColumn(row, inputCol).asLong().getLong();
                colValues.add(val);
            } catch (Exception ignored) {
            }
        }
        if (colValues.isEmpty()) {
            return null;
        }
        Collections.sort(colValues);
        DataTable rv = createTable(colValues.size());
        for (Bundle row : result) {
            try {
                int index = 1 + colValues.indexOf(binder.getColumn(row, inputCol).asLong().getLong());
                ValueObject rank = ValueFactory.create((double) index / colValues.size());
                if (outputCol >= row.getCount()) {
                    binder.appendColumn(row, rank);
                } else {
                    binder.setColumn(row, outputCol, rank);
                }
                rv.append(row);
            } catch (Exception ignored) {
            }

        }
        return rv;
    }
}

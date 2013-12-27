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

import java.util.Map;
import java.util.Map.Entry;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractRowOp;
import com.addthis.hydra.data.util.KeyHistogram;


/**
 * <p>This query operation <span class="hydra-summary">builds a histogram for a column</span>.
 * <p>The syntax of the operation is "histo=[column],[scale]. The scale represents the
 * base value in the exponential scale of buckets. For example a scale value of 10 would
 * yield buckets: 1-9, 10-99, 100-999, etc. The result of this operation is a table with two columns.
 * Column 0 stores the lower bound of the bucket and column 1 stores the number of elements
 * within the bucket.
 *
 * @user-reference
 * @hydra-name histo
 */
public class OpHistogram extends AbstractRowOp {

    private final int scale;
    private final int column;

    /**
     * usage: column, scale
     * <p/>
     * column defines the column source for the bucket value.
     * scale determines the power value for bucket sizing.
     *
     * @param args
     */
    public OpHistogram(String args) {
        int v[] = csvToInts(args);
        if (v.length < 1) {
            throw new RuntimeException("missing required column");
        }
        column = v[0];
        scale = v.length > 1 ? v[1] : 10;

        histo = new KeyHistogram().setScale(scale).init();
    }

    KeyHistogram histo;
    BundleColumnBinder binder;
    Bundle rowFactory;

    @Override
    public Bundle rowOp(Bundle row) {
        if (binder == null) {
            binder = getSourceColumnBinder(row);
            rowFactory = row.createBundle();
        }
        histo.update(0, ValueUtil.asNumberOrParse(binder.getColumn(row, column)).asLong().getLong());
        return null;
    }

    @Override
    public void sendComplete() {
        Map<Long, Long> map = histo.getSortedHistogram();
        for (Entry<Long, Long> e : map.entrySet()) {
            Bundle row = rowFactory.createBundle();
            binder.appendColumn(row, ValueFactory.create(e.getKey()));
            binder.appendColumn(row, ValueFactory.create(e.getValue()));
            getNext().send(row);
        }
        super.sendComplete();
    }
}

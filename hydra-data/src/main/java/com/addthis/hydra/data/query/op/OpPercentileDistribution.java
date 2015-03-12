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
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.IndexField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.util.KeyPercentileDistribution;

import com.yammer.metrics.stats.Snapshot;

import io.netty.channel.ChannelProgressivePromise;

/**
 * <p>This query operation <span class="hydra-summary">calculates the percentile distribution of a column</span>.
 * <p/>
 * <p>The syntax for the operation is distribution=[column number],[sample size]. The sample
 * size is optional and the default sample size is 1028. The result of this operation is a table
 * with two columns. Column 0 has percentile distributions and column 1 has the counts for
 * those percentile distributions.</p>
 *
 * @user-reference
 * @hydra-name distribution
 */
public class OpPercentileDistribution extends AbstractTableOp {

    private final int sampleSize;
    private final AutoField column;

    /**
     * usage: column, sampleSize
     * <p/>
     * column defines the column source for the percentile value
     * sampleSize determines the size of the sample set to use when calculating percentiles
     *
     * @param tableFactory
     * @param args
     */
    public OpPercentileDistribution(DataTableFactory tableFactory, String args, ChannelProgressivePromise queryPromise) {
        super(tableFactory, queryPromise);
        int[] v = csvToInts(args);
        if (v.length < 1) {
            throw new RuntimeException("missing required column");
        }
        column = new IndexField(v[0]);
        if (v.length > 1) {
            sampleSize = v[1];
        } else {
            sampleSize = 1028;
        }
    }

    @Override
    public DataTable tableOp(DataTable result) {
        KeyPercentileDistribution histo = new KeyPercentileDistribution(sampleSize).init();
        // build histogram
        for (Bundle row : result) {
            long ev = column.getLong(row).getAsLong();
            histo.update(ev);
        }
        BundleFormat tableFormat = result.getFormat();
        int existingCols = tableFormat.getFieldCount();
        ensureMinimumFieldCount(tableFormat, existingCols + 2);
        AutoField label = new IndexField(existingCols);
        AutoField value = new IndexField(existingCols + 1);
        // output
        Snapshot snapshot = histo.getSnapshot();
        DataTable resultTable = createTable(6);
        bindColumn(label, value, resultTable, ".5", snapshot.getMedian());
        bindColumn(label, value, resultTable, ".75", snapshot.get75thPercentile());
        bindColumn(label, value, resultTable, ".95", snapshot.get95thPercentile());
        bindColumn(label, value, resultTable, ".98", snapshot.get98thPercentile());
        bindColumn(label, value, resultTable, ".99", snapshot.get99thPercentile());
        bindColumn(label, value, resultTable, ".999", snapshot.get999thPercentile());

        return resultTable;
    }

    private void ensureMinimumFieldCount(BundleFormat format, int targetCount) {
        int suffixNum = 0;
        while (format.getFieldCount() < targetCount) {
            format.getField("__op_percent_dist_anon_" + suffixNum);
        }
    }

    private void bindColumn(AutoField labelField,
                            AutoField valueField,
                            DataTable resultTable,
                            String label,
                            double value) {
        Bundle bundle = resultTable.createBundle();
        labelField.setValue(bundle, ValueFactory.create(label));
        valueField.setValue(bundle, ValueFactory.create(value));
        resultTable.add(bundle);
    }
}

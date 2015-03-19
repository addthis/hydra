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
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.IndexField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractQueryOp;
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
public class OpPercentileDistribution extends AbstractQueryOp {

    private final KeyPercentileDistribution histo;
    private final AutoField column;
    private final BundleFactory bundleFactory;

    /**
     * usage: column, sampleSize
     * <p/>
     * column defines the column source for the percentile value
     * sampleSize determines the size of the sample set to use when calculating percentiles
     */
    public OpPercentileDistribution(BundleFactory bundleFactory, String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.bundleFactory = bundleFactory;
        int[] v = csvToInts(args);
        if (v.length < 1) {
            throw new RuntimeException("missing required column");
        }
        column = new IndexField(v[0]);
        int sampleSize;
        if (v.length > 1) {
            sampleSize = v[1];
        } else {
            sampleSize = 1028;
        }
        histo = new KeyPercentileDistribution(sampleSize).init();
    }

    @Override public void send(Bundle bundle) {
        long ev = column.getLong(bundle).getAsLong();
        histo.update(ev);
    }

    @Override public void sendComplete() {
        // prep bundle format
        Bundle bundle = bundleFactory.createBundle();
        BundleFormat tableFormat = bundle.getFormat();
        ensureMinimumFieldCount(tableFormat, 2);
        AutoField label = new IndexField(0);
        AutoField value = new IndexField(1);
        // output
        Snapshot snapshot = histo.getSnapshot();
        writeLine(label, value, bundle, ".5", snapshot.getMedian());
        writeLine(label, value, bundleFactory.createBundle(), ".75", snapshot.get75thPercentile());
        writeLine(label, value, bundleFactory.createBundle(), ".95", snapshot.get95thPercentile());
        writeLine(label, value, bundleFactory.createBundle(), ".98", snapshot.get98thPercentile());
        writeLine(label, value, bundleFactory.createBundle(), ".99", snapshot.get99thPercentile());
        writeLine(label, value, bundleFactory.createBundle(), ".999", snapshot.get999thPercentile());
    }

    private void writeLine(AutoField labelField,
                           AutoField valueField,
                           Bundle bundle,
                           String label,
                           double value) {
        labelField.setValue(bundle, ValueFactory.create(label));
        valueField.setValue(bundle, ValueFactory.create(value));
        getNext().send(bundle);
    }

    private static void ensureMinimumFieldCount(BundleFormat format, int targetCount) {
        int suffixNum = 0;
        while (format.getFieldCount() < targetCount) {
            format.getField("__op_percent_dist_anon_" + suffixNum);
        }
    }
}

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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractTableOp;

import com.google.common.base.Objects;

import io.netty.channel.ChannelProgressivePromise;

/**
 * Treat columns as belonging to a frequency table and try to
 * calculate percentiles and such
 */
public class OpFrequencyTable extends AbstractTableOp {

    private String[] cols;
    private int valueIndex;
    private int freqIndex;
    private Double[] desiredPercentiles = new Double[1]; // boo boxing
    private boolean appendTotal = false;

    // foo=0,1,2,3:4,5:0.99,p12
    public OpFrequencyTable(DataTableFactory processor, String args, ChannelProgressivePromise queryPromise) {
        super(processor, queryPromise);
        String[] tuple = Strings.splitArray(args, ":");
        cols = Strings.splitArray(tuple[0], ",");

        valueIndex = Integer.valueOf(Strings.splitArray(tuple[1], ",")[0]);
        freqIndex = Integer.valueOf(Strings.splitArray(tuple[1], ",")[1]);

        String[] pcols = Strings.splitArray(tuple[2], ",");

        List<Double> percentiles = new ArrayList<>();
        for (int i = 0; i < pcols.length; i++) {
            if ("total".equals(pcols[i])) {
                appendTotal = true;
            } else {
                percentiles.add(Double.valueOf(pcols[i]));
            }
        }
        desiredPercentiles = percentiles.toArray(desiredPercentiles);
    }


    @Override
    public DataTable tableOp(DataTable result) {
        BundleColumnBinder rowBinder;
        BundleColumnBinder deckBinder;
        BundleField[] keyColumns;
        BundleField valueColumn;
        BundleField freqColumn;

        DataTable table = createTable(0);
        FTable recentFTable = new FTable();
        Bundle onDeck = null;
        if (result.size() > 0) {
            onDeck = result.get(0);
        } else {
            return table;
        }

        // try
        // {

        for (int i = 0; i < result.size(); i++) {
            boolean lastRow = i == result.size() - 1;
            Bundle row = result.get(i);
            rowBinder = new BundleColumnBinder(row, cols);
            deckBinder = new BundleColumnBinder(onDeck);
            keyColumns = rowBinder.getFields();
            valueColumn = row.getFormat().getField(Integer.toString(valueIndex));
            freqColumn = row.getFormat().getField(Integer.toString(freqIndex));
            boolean eqCol = equalColumnKeys(keyColumns, onDeck, row);
            //System.out.println("foo " + row + " deck " + onDeck + " freq " + recentFTable);
            if (eqCol) {
                recentFTable.update(ValueUtil.asNumberOrParseLong(row.getValue(valueColumn), 10).asLong().getLong(),
                        ValueUtil.asNumberOrParseLong(row.getValue(freqColumn), 10).asLong().getLong());
            }

            if (!eqCol || lastRow) {
                onDeck.removeValue(valueColumn);
                onDeck.removeValue(freqColumn);
                if (appendTotal) {
                    deckBinder.appendColumn(onDeck, ValueFactory.create(recentFTable.getTotalEntries()));
                }
                for (int j = 0; j < desiredPercentiles.length; j++) {
                    long percentile = recentFTable.getNearestPercentile(desiredPercentiles[j]);
                    deckBinder.appendColumn(onDeck, ValueFactory.create(percentile));
                }
                table.append(onDeck);
                if (onDeck != row) {
                    recentFTable = new FTable();
                    recentFTable.update(ValueUtil.asNumberOrParseLong(row.getValue(valueColumn), 10).asLong().getLong(),
                            ValueUtil.asNumberOrParseLong(row.getValue(freqColumn), 10).asLong().getLong());
                }
                onDeck = row;
                if (lastRow && !eqCol) {
                    onDeck.removeValue(valueColumn);
                    onDeck.removeValue(freqColumn);
                    if (appendTotal) {
                        deckBinder.appendColumn(onDeck, ValueFactory.create(recentFTable.getTotalEntries()));
                    }
                    for (int j = 0; j < desiredPercentiles.length; j++) {
                        long percentile = recentFTable.getNearestPercentile(desiredPercentiles[j]);
                        deckBinder.appendColumn(onDeck, ValueFactory.create(percentile));
                    }
                    table.append(onDeck);
                }

            }
            //System.out.println("bar " + row + " deck " + onDeck + " freq " + recentFTable);
        }
        // }
        // catch(Exception e)
        // {
        //  e.printStackTrace();
        // }
        return table;
    }


    public static boolean equalColumnKeys(BundleField[] keyColumns, Bundle oldb, Bundle newb) {
        boolean eq = true;
        for (int i = 0; i < keyColumns.length; i++) {
            String oldstr = ValueUtil.asNativeString(oldb.getValue(keyColumns[i]));
            String newstr = ValueUtil.asNativeString(newb.getValue(keyColumns[i]));

            if (oldstr != null && newstr != null && oldstr.equals(newstr)) {
                ;
            } else {
                eq = false;
            }
        }
        return eq;
    }


    // todo: floats, ints and doubles too?
    public static class FTable {

        private long totalEntries;
        // todo: use some fancy primitive collection
        private SortedMap<Long, Long> freqMap;

        public FTable() {
            totalEntries = 0;
            freqMap = new TreeMap<>();
        }

        public void update(Long value, Long freq) {
            if (freqMap.containsKey(value)) {
                freqMap.put(value, freqMap.get(value) + freq);
            } else {
                freqMap.put(value, freq);
            }
            totalEntries += freq;
        }

        public long getTotalEntries() {
            return totalEntries;
        }

        // todo: mean of odd values?
        public long getNearestPercentile(double p) {
            long pindex = Math.max(0, Math.round(p * totalEntries));
            long index = 0;
            for (Map.Entry<Long, Long> entry : freqMap.entrySet()) {
                index += entry.getValue();
                if (index >= pindex) {
                    return entry.getKey();
                }
            }
            return -1; // something went wrong
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("totalEntries", totalEntries)
                    .add("freqMap", freqMap)
                    .toString();
        }
    }

}

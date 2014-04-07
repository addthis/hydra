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

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.op.merge.AverageValue;
import com.addthis.hydra.data.query.op.merge.BundleMapConf;
import com.addthis.hydra.data.query.DiskBackedMap;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.data.query.op.MergedRowFactory;
import com.addthis.hydra.data.query.op.merge.DiffValue;
import com.addthis.hydra.data.query.op.merge.JoinedValue;
import com.addthis.hydra.data.query.op.merge.KeyValue;
import com.addthis.hydra.data.query.op.merge.LastValue;
import com.addthis.hydra.data.query.op.merge.MaxValue;
import com.addthis.hydra.data.query.op.merge.MergedValue;
import com.addthis.hydra.data.query.op.merge.MinValue;
import com.addthis.hydra.data.query.op.merge.NumMergesValue;
import com.addthis.hydra.data.query.op.merge.SumValue;
import com.addthis.hydra.data.util.KeyTopper;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * <p>This query operation <span class="hydra-summary">merges arbitrary rows</span>.
 * <p/>
 * <p>Gather collects all rows that match the criteria of the key columns.
 * It is an in-memory operation that spill over to disk when necessary. If the key
 * columns are already sorted then the {@link OpMerge merge} operation is
 * a much cheaper alternative.</p>
 * <p>The syntax for this operation is "gather=[column parameters] where
 * column parameters is a sequence of one or more of the following letters:
 * <ul>
 * <li>k - this column is a key column.</li>
 * <li>i - this column is ignored and dropped from the output.</li>
 * <li>t - this column is a key topper.</li>
 * <li>a - generate average values for this column</li>
 * <li>d - generate iterated diff values for this column</li>
 * <li>m - generate min values for this column</li>
 * <li>M - generate max values for this column</li>
 * <li>s - generate sum values for this column</li>
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
 * gather=iks
 *
 * A 9
 * B 2
 * </pre>
 *
 * @user-reference
 * @hydra-name gather
 */
public class OpGather extends AbstractQueryOp {

    public static ValueNumber num(ValueObject o) {
        ValueNumber num = ValueUtil.asNumberOrParseLong(o, 10);
        return num != null ? num : ZERO;
    }

    private Map<String, MergedRow> resultTable = new HashMap<>();
    private final ListBundleFormat format = new ListBundleFormat();
    private final BundleMapConf<MergedValue> conf[];

    private final long tipMem;
    private final long tipRow;
    private long totalMem;

    private BundleMapConf<MergedValue> mergeCountValue;
    private KeyTopper topper;
    private int topSize;
    private int topColumn = -1;

    private boolean tippedToDisk = false;
    private boolean tipToDisk = Parameter.boolValue("opgather.tiptodisk", false);

    private String tmpDir = "opgather.tmp";

    private static final Meter diskTips = Metrics.newMeter(OpGather.class, "diskTips", "diskTips", TimeUnit.SECONDS);

    final QueryStatusObserver queryStatusObserver;

    public OpGather(String args, long tipMem, long tipRow, String tmpDir, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        this.tmpDir = tmpDir;
        this.tipMem = tipMem;
        this.tipRow = tipRow;
        totalMem = 0;

        ArrayList<BundleMapConf<MergedValue>> conf = new ArrayList<>(args.length());

        boolean mergeCount = false;
        for (int i = 0; i < args.length(); i++) {
            char ch = args.charAt(i);
            boolean isnum = (ch >= '0' && ch <= '9');
            if (isnum) {
                topSize *= 10;
                topSize += (ch - '0');
                continue;
            }
            MergedValue op = null;
            // TODO add max/min
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
                // pack repeating values -- seems to have been gone a while
//                case 'p':
//                    op = MergeOpEnums.PACK;
//                    break;
                // sum
                case 's':
                    op = new SumValue();
                    break;
                // add merged row count
                case 'u':
                    mergeCount = true;
                    continue;
            }
            if (op != null) {
                BundleMapConf<MergedValue> next = new BundleMapConf<>();
                next.setOp(op);
                conf.add(next);
            } else {
                conf.add(null);
            }
        }
        if (mergeCount) {
            mergeCountValue = new BundleMapConf<>();
            mergeCountValue.setOp(new NumMergesValue());
            conf.add(mergeCountValue);
        }
        this.conf = conf.toArray(new BundleMapConf[conf.size()]);
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        if (queryStatusObserver.queryCompleted) {
            return;
        }
        String key = "";
        int i = 0;
        /* compute key for new line and fill from/to if not set */
        BundleField topField = null;
        for (BundleField field : row.getFormat()) {
            if (i >= conf.length) {
                break;
            }
            if (i == topColumn) {
                topField = field;
            }
            BundleMapConf<MergedValue> mc = conf[i++];
            if (mc == null) {
                continue;
            }
            if (mc.getFrom() == null) {
                mc.setFrom(field);
                // TODO only clone field name for non-int names, otherwise create 'next' column # as name
                mc.setTo(format.getField(field.getName()));
            }
            if (mc.getOp().isKey()) {
                ValueObject lval = row.getValue(field);
                key = key.concat(lval == null ? "" : lval.toString());
            }
        }
        if (mergeCountValue != null) {
            if (mergeCountValue.getTo() == null) {
                mergeCountValue.setTo(format.createNewField("merge_"));
            }
        }
        MergedRow merge = resultTable.get(key);
        if (merge == null) {
            merge = new MergedRow(this);
            resultTable.put(key, merge);

            if (!tippedToDisk) {
                totalMem += MemoryCounter.estimateSize(merge);
            }
        }
        if (!tippedToDisk) {
            totalMem -= MemoryCounter.estimateSize(merge);
        }
        merge.merge(row);
        if (tippedToDisk) {
            // Update the result on the disk, we need to put again
            resultTable.put(key, merge);
        }
        if (!tippedToDisk) {
            totalMem += MemoryCounter.estimateSize(merge);
        }

        if (topField != null) {
            ValueNumber num = num(merge.mergedRow[topColumn]);
            if (num == null) {
                return;
            }
            String drop = topper.update(key, num.asLong().getLong(), topSize);
            if (drop != null) {
                if (!tippedToDisk) {
                    totalMem -= MemoryCounter.estimateSize(resultTable.get(drop));
                }

                resultTable.remove(drop);
            }
        }

        if (!tipToDisk) {
            // If we're not tipping to disk, and the tips are set, then we will issue errors if we pass them
            if (tipMem > 0 && totalMem > tipMem) {
                throw new DataChannelError("Memory usage of gathered objects exceeds allowed " + tipMem);
            }

            if (tipRow > 0 && resultTable.size() > tipRow) {
                throw new DataChannelError("Number of gathered rows exceeds allowed " + tipRow);
            }
        } else {
            // If we're tipping to disk, and the tips are non zero, then spill to disk once we pass them
            if (!tippedToDisk && ((tipMem > 0 && totalMem > tipMem) || (tipRow > 0 && resultTable.size() > tipRow))) {
                tippedToDisk = true;
                diskTips.mark();

                // Use the smaller amount of memory for the JE cache environment
                long memToUse = totalMem;
                if (memToUse > tipMem) {
                    memToUse = tipMem;
                }

                Map<String, MergedRow> diskMap = new DiskBackedMap<>(tmpDir + "/" + UUID.randomUUID(),
                        new MergedRowFactory(this), memToUse);

                diskMap.putAll(resultTable);
                resultTable = diskMap;
            }
        }
    }

    @Override
    public void sendComplete() {
        QueryOp next = getNext();
        for (MergedRow mergedRow : resultTable.values()) {
            if (!queryStatusObserver.queryCompleted) {
                next.send(mergedRow.emit());
            } else {
                break;
            }
        }
        next.sendComplete();
    }

    @Override
    public void close() throws IOException {
        if (resultTable instanceof DiskBackedMap) {
            ((DiskBackedMap<MergedRow>) resultTable).close();
        }

        super.close();
    }
}

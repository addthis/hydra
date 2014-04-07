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
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.op.merge.BundleMapConf;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.QueryStatusObserver;


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

    //  private MergeOpEnums op[];
    private BundleField mergeField;
    private boolean mergeCount;
    private int countdown;
    private String join[];
    private int cols;
    private final ListBundleFormat format = new ListBundleFormat();
    private final BundleMapConf<MergeOpEnums> conf[];
    private final HashMap<String, MergedRow> resultTable = new HashMap<>();
    private final QueryStatusObserver queryStatusObserver;

    public OpMerge(String args, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        ArrayList<BundleMapConf<MergeOpEnums>> conf = new ArrayList<>(args.length());

        MergeOpEnums lastOp = null;
        StringBuilder newJoin = null;
        boolean wasnum = false;
        for (int i = 0; i < args.length(); i++) {
            char ch = args.charAt(i);
            boolean isnum = (ch >= '0' && ch <= '9');
            MergeOpEnums op = null;
            MergeOpEnums nextOp = null;
            if (isnum) {
                countdown *= 10;
                countdown += (ch - '0');
            } else if (wasnum) {
                nextOp = MergeOpEnums.COUNTDOWN;
            }
            wasnum = isnum;
            boolean advance = true;
            // TODO add max/min

            switch (ch) {
                // skip comma prettifiers
                case ',':
                    advance = false;
                    break;
                // average
                case 'a':
                    op = MergeOpEnums.AVG;
                    break;
                // diff/subtract value
                case 'd':
                    op = MergeOpEnums.DIFF;
                    break;
                // ignore/drop
                case 'i':
                    op = MergeOpEnums.IGNORE;
                    break;
                // last value
                case 'l':
                    op = MergeOpEnums.LAST;
                    break;
                // string join
                case 'j':
                    if (join == null) {
                        join = new String[args.length()];
                    }
                    join[cols] = ",";
                    op = MergeOpEnums.JOIN;
                    break;
                // part of compound key
                case 'k':
                    op = MergeOpEnums.KEY;
                    break;
                // max value
                case 'M':
                    op = MergeOpEnums.MAX;
                    break;
                // min value
                case 'm':
                    op = MergeOpEnums.MIN;
                    break;
                // pack repeating values
                case 'p':
                    op = MergeOpEnums.PACK;
                    break;
                // sum
                case 's':
                    op = MergeOpEnums.SUM;
                    break;
                // add merged row count
                case 'u':
                    op = MergeOpEnums.UCOUNT;
                    mergeCount = true;
                    break;
                default:
                    if (lastOp == MergeOpEnums.JOIN) {
                        if (newJoin == null) {
                            newJoin = new StringBuilder();
                        }
                        newJoin.append(ch);
                    }
                    if (i != args.length() - 1) {
                        continue;
                    } else {
                        advance = false;
                        break;
                    }
            }
            if (op != null) {
                addBundleMapConf(conf, op);
            } else {
                conf.add(null);
            }
            if (nextOp != null) {
                addBundleMapConf(conf, nextOp);
            }
            if (lastOp == MergeOpEnums.JOIN && newJoin != null) {
                join[cols - 1] = newJoin.toString();
                newJoin = null;
            }
            if (advance) {
                lastOp = op;
                cols++;
            }
        }
        this.conf = conf.toArray(new BundleMapConf[conf.size()]);
    }

    private void addBundleMapConf(ArrayList<BundleMapConf<MergeOpEnums>> conf, MergeOpEnums op) {
        BundleMapConf<MergeOpEnums> next = new BundleMapConf<>();
        next.setOp(op);
        conf.add(next);
    }

    String lastkey = null;
    int rows = 0;

    @Override
    public void send(Bundle bundle) {
        if (queryStatusObserver.queryCompleted) {
            return;
        }

        String key = "";
        int i = 0;
        for (BundleField bundleField : bundle) {
            if (i >= conf.length) {
                break;
            }
            BundleMapConf<MergeOpEnums> mc = conf[i++];
            if (mc == null) {
                continue;
            }
            if (mc.getFrom() == null && !(mc.getOp().equals(MergeOpEnums.IGNORE) || mc.getOp().equals(MergeOpEnums.UCOUNT))) {
                mc.setFrom(bundleField);
                // TODO only clone field name for non-int names, otherwise create 'next' column # as name
                mc.setTo(format.getField(bundleField.getName()));
            }
            if (mc.getOp() == MergeOpEnums.KEY) {
                ValueObject lval = bundle.getValue(bundleField);
                key = key.concat(lval == null ? "" : lval.toString());
            }
        }
        if (mergeCount && mergeField == null) {
            mergeField = format.createNewField("merge_");
        }
        MergedRow merge = resultTable.get(key);
        if (merge == null) {
            merge = new MergedRow(this, conf, mergeCount, join, format, mergeField);
            resultTable.put(key, merge);
        }
        for (BundleField field : bundle) {
            ValueObject lval = bundle.getValue(field);
            if (i < conf.length) {
                BundleMapConf<MergeOpEnums> mc = conf[i++];
                if (mc == null || mc.getOp() == null) {
                    continue;
                }
                switch (mc.getOp()) {
                    case KEY:
                        key = key.concat(lval == null ? "" : lval.toString());
                        break;
                }
            }
        }
        if (rows++ == 0) {
            lastkey = key;
        }
        merge.merge(bundle);
        if (!lastkey.equals(key) || (countdown > 0 && merge.merged >= countdown)) {
            getNext().send(resultTable.remove(lastkey).emit());
            lastkey = key;
        }
    }

    @Override
    public void sendComplete() {
        QueryOp next = getNext();
        if (!queryStatusObserver.queryCompleted) {
            List<Bundle> bundleList = new ArrayList<>();
            for (MergedRow mergedRow : resultTable.values()) {
                bundleList.add(mergedRow.emit());
            }
            next.send(bundleList);
        }
        resultTable.clear();
        next.sendComplete();
    }

    @Override
    public void close() throws IOException {
        resultTable.clear();
        if (getNext() != null) {
            getNext().close();
        }
    }

    static ValueNumber num(ValueObject o) {
        return OpGather.num(o);
    }
}

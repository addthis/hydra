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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


public class OpRoll extends AbstractRowOp {

    public static enum OP {
        MIN, MAX, AVG, SUM, DELTA
    }

    /**
     * <p>This query operation <span class="hydra-summary">calculates the minimum value</span>.
     * <p/>
     * <p>This operation keeps track of the currently observed minimum value in one
     * or more columns. The list of input columns are comma-separated.
     * For each input column an output column will be generated (unless the 's'
     * prefix is used see below).
     * <p>Optionally a list of comma-separated key columns
     * can be specified with a colon ":" following the list of input columns. If the
     * <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a>
     * of adjacent rows is different then the minimum values are reset.
     * <p>An optional prefix of 'i' or 'f' before the input columns
     * designates whether to process as ints or floats (defaults to
     * ints). Prefix with 's' to makes changes swap value in place. Prefix
     * with 'S' causes the operation to not emit any columns during processing and
     * emit an additional row at the end of processing with the final maximum value.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     min=0    // generate a new column that emits the current min observed for column 0
     *     min=0,3  // generate one column for the min of column 0 and one column for the min of column 3
     *     min=0:1  // track the min of column 0. If adjacent rows in column 1 are different then reset the state
     *     min=s0   // overwrite column 0 with the current minimum value
     * </pre>
     *
     * @user-reference
     * @hydra-name min
     */
    public static class MinOpRoll extends OpRoll {

        public MinOpRoll(String args, ChannelProgressivePromise queryPromise) {
            super(args, OP.MIN, queryPromise);
        }
    }

    /**
     * <p>This query operation <span class="hydra-summary">calculates the maximum value</span>.
     * <p/>
     * <p>This operation keeps track of the currently observed maximum value in one
     * or more columns. The list of input columns are comma-separated.
     * For each input column an output column will be generated (unless the 's'
     * prefix is used see below).
     * <p>Optionally a list of comma-separated key columns
     * can be specified with a colon ":" following the list of input columns. If the
     * <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a>
     * of adjacent rows is different then the maximum values are reset.
     * <p>An optional prefix of 'i' or 'f' before the input columns
     * designates whether to process as ints or floats (defaults to
     * ints). Prefix with 's' to makes changes swap value in place. Prefix
     * with 'S' causes the operation to not emit any columns during processing and
     * emit an additional row at the end of processing with the final maximum value.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     max=0    // generate a new column that emits the current max observed for column 0
     *     max=0,3  // generate one column for the max of column 0 and one column for the max of column 3
     *     max=0:1  // track the max of column 0. If adjacent rows in column 1 are different then reset the state
     *     max=s0   // overwrite column 0 with the current maximum value
     * </pre>
     *
     * @user-reference
     * @hydra-name max
     */
    public static class MaxOpRoll extends OpRoll {

        public MaxOpRoll(String args, ChannelProgressivePromise queryPromise) {
            super(args, OP.MAX, queryPromise);
        }
    }

    /**
     * <p>This query operation <span class="hydra-summary">calculates the average value</span>.
     * <p/>
     * <p>This operation keeps track of the currently observed average value in one
     * or more columns. The list of input columns are comma-separated.
     * For each input column an output column will be generated (unless the 's'
     * prefix is used see below).
     * <p>Optionally a list of comma-separated key columns
     * can be specified with a colon ":" following the list of input columns. If the
     * <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a>
     * of adjacent rows is different then the average values are reset.
     * <p>An optional prefix of 'i' or 'f' before the input columns
     * designates whether to process as ints or floats (defaults to
     * ints). Prefix with 's' to makes changes swap value in place. Prefix
     * with 'S' causes the operation to not emit any columns during processing and
     * emit an additional row at the end of processing with the final average value.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     avg=0    // generate a new column that emits the current average observed for column 0
     *     avg=0,3  // generate one column for the average of column 0 and one column for the average of column 3
     *     avg=0:1  // track the average of column 0. If adjacent rows in column 1 are different then reset the state
     *     avg=s0   // overwrite column 0 with the current average value
     * </pre>
     *
     * @user-reference
     * @hydra-name avg
     */
    public static class AvgOpRoll extends OpRoll {

        public AvgOpRoll(String args, ChannelProgressivePromise queryPromise) {
            super(args, OP.AVG, queryPromise);
        }
    }

    /**
     * <p>This query operation <span class="hydra-summary">calculates the sum of values</span>.
     * <p/>
     * <p>This operation keeps track of the currently observed sum in one
     * or more columns. The list of input columns are comma-separated.
     * For each input column an output column will be generated (unless the 's'
     * prefix is used see below).
     * <p>Optionally a list of comma-separated key columns
     * can be specified with a colon ":" following the list of input columns. If the
     * <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a>
     * of adjacent rows is different then the sum values are reset.
     * <p>An optional prefix of 'i' or 'f' before the input columns
     * designates whether to process as ints or floats (defaults to
     * ints). Prefix with 's' to makes changes swap value in place. Prefix
     * with 'S' causes the operation to not emit any columns during processing and
     * emit an additional row at the end of processing with the final sum.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     sum=0    // generate a new column that emits the current sum observed for column 0
     *     sum=0,3  // generate one column for the sum of column 0 and one column for the sum of column 3
     *     sum=0:1  // track the sum of column 0. If adjacent rows in column 1 are different then reset the state
     *     sum=s0   // overwrite column 0 with the current sum
     * </pre>
     *
     * @user-reference
     * @hydra-name sum
     */
    public static class SumOpRoll extends OpRoll {

        public SumOpRoll(String args, ChannelProgressivePromise queryPromise) {
            super(args, OP.SUM, queryPromise);
        }
    }

    /**
     * <p>This query operation <span class="hydra-summary">calculates the delta of values</span>.
     * <p/>
     * <p>This operation keeps track of the delta from the previous value in one
     * or more columns. The list of input columns are comma-separated.
     * For each input column an output column will be generated (unless the 's'
     * prefix is used see below).
     * <p>Optionally a list of comma-separated key columns
     * can be specified with a colon ":" following the list of input columns. If the
     * <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a>
     * of adjacent rows is different then the delta values are reset.
     * <p>An optional prefix of 'i' or 'f' before the input columns
     * designates whether to process as ints or floats (defaults to
     * ints). Prefix with 's' to makes changes swap value in place. Prefix
     * with 'S' causes the operation to not emit any columns during processing and
     * emit an additional row at the end of processing with the final delta.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     delta=0    // generate a new column that emits the current delta observed for column 0
     *     delta=0,3  // generate one column for the delta of column 0 and one column for the delta of column 3
     *     delta=0:1  // track the delta of column 0. If adjacent rows in column 1 are different then reset the state
     *     delta=s0   // overwrite column 0 with the current delta
     * </pre>
     *
     * @user-reference
     * @hydra-name delta
     */
    public static class DeltaOpRoll extends OpRoll {

        public DeltaOpRoll(String args, ChannelProgressivePromise queryPromise) {
            super(args, OP.DELTA, queryPromise);
        }
    }


    private final String[] args;
    private final OP op;
    private final boolean asInt;
    private final boolean inPlace;
    private final boolean summary;

    private Numeric[] state;
    private BundleField[] colIn;
    private BundleField[] colOut;
    private BundleField[] colKeys;
    private int rows;
    private String lastKey;
    private Numeric[] oldvals;
    private Bundle lastRow;

    public OpRoll(String args, OP op, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        boolean asInt = true;
        this.op = op;
        if (args.startsWith("i")) {
            args = args.substring(1);
        }
        if (args.startsWith("f")) {
            asInt = false;
            args = args.substring(1);
        }
        if (args.startsWith("s") || args.startsWith("S")) {
            inPlace = args.startsWith("s");
            summary = !inPlace;
            args = args.substring(1);
        } else {
            summary = false;
            inPlace = false;
        }
        this.args = Strings.splitArray(args, ":");
        this.asInt = asInt;
    }

    private final Numeric toType(ValueObject vo) {
        if (asInt) {
            return ValueUtil.asNumberOrParseLong(vo, 10);
        } else {
            return ValueUtil.asNumberOrParseDouble(vo);
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (state == null) {
            colIn = new BundleColumnBinder(row, Strings.splitArray(args[0], ",")).getFields();
            colKeys = args.length > 1 ? new BundleColumnBinder(row, Strings.splitArray(args[1], ",")).getFields() : null;
            state = new Numeric[colIn.length];
            oldvals = new Numeric[colIn.length];
            if (inPlace || summary) {
                colOut = colIn;
                if (summary) {
                    lastRow = row.createBundle();
                }
            } else {
                colOut = new BundleField[colIn.length];
                for (int i = 0; i < colOut.length; i++) {
                    colOut[i] = row.getFormat().getField("op_".concat(colIn[i].getName()));
                }
            }
        }
        rows++;
        if (colKeys != null) {
            String key = createCompoundKey(colKeys, row);
            if ((lastKey == null && key != null) || (lastKey != null && !lastKey.equals(key))) {
                lastKey = key;
                for (int j = 0; j < state.length; j++) {
                    state[j] = null;
                }
            }
        }
        BundleColumnBinder binder = this.getSourceColumnBinder(row);
        for (int i = 0; i < colIn.length; i++) {
            if (state[i] == null) {
                state[i] = toType(row.getValue(colIn[i]));
                if (state[i] == null) {
                    state[i] = ZERO;
                }
                oldvals[i] = state[i];
            } else {
                switch (op) {
                    case DELTA:
                        Numeric newval = toType(row.getValue(colIn[i]));
                        state[i] = newval.diff(oldvals[i]);
                        oldvals[i] = newval;
                        break;
                    case MIN:
                        state[i] = toType(state[i].min(toType(row.getValue(colIn[i]))));
                        break;
                    case MAX:
                        state[i] = toType(state[i].max(toType(row.getValue(colIn[i]))));
                        break;
                    case SUM:
                        state[i] = state[i].sum(toType(row.getValue(colIn[i])));
                        break;
                    case AVG:
                        state[i] = toType(state[i].sum(toType(row.getValue(colIn[i]))));
                        break;
                }
            }
            if (!summary) {
                if (op == OP.AVG) {
                    row.setValue(colOut[i], state[i].avg(rows));
                } else {
                    row.setValue(colOut[i], state[i]);
                }
            }
        }
        return row;
    }

    @Override
    public void sendComplete() {
        if (summary && state != null) {
            for (int i = 0; i < state.length; i++) {
                if (op == OP.AVG) {
                    lastRow.setValue(colOut[i], state[i].avg(rows));
                } else {
                    lastRow.setValue(colOut[i], state[i]);
                }
            }
            getNext().send(lastRow);
        }
        super.sendComplete();
    }
}

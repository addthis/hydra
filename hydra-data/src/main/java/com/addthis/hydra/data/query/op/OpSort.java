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

import java.util.Comparator;
import java.util.StringTokenizer;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">performs an in-memory sort that uses disk
 * when necessary</span>.
 * <p/>
 * <p>The syntax is sort=[cols]:[type]:[direction]. [cols] is one or more columns
 * separated by commas. Type is a sequence of
 * ["i" or "l" or "n"] for integer number, ["d" or "f"] for floating point number, or "s" for string.
 * Direction is a sequence of either "a" for ascending or "d" for descending.
 * The columns are delimited by commas and the types and directions have
 * no delimiter. The lengths
 * of [type] and [direction] must be equal to the number of columns specified.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * 0 A 3
 * 1 A 1
 * 1 B 2
 * 0 A 5
 *
 * sort=0,1,2:nsn:add
 *
 * 0 A 5
 * 0 A 3
 * 1 B 2
 * 1 A 1
 * </pre>
 *
 * @user-reference
 * @hydra-name sort
 */
public class OpSort extends AbstractTableOp {

    private final String[] cols;
    private final char[] type;
    private final char[] dir;

    public OpSort(DataTableFactory factory, String args, ChannelProgressivePromise queryPromise) {
        super(factory, queryPromise);

        StringTokenizer st = new StringTokenizer(args, ":");
        cols = LessStrings.splitArray(st.hasMoreElements() ? st.nextToken() : "0", ",");

        String ts = st.hasMoreElements() ? st.nextToken() : "s";
        while (ts.length() < cols.length) {
            ts = ts.concat(ts.substring(0, 1));
        }
        type = ts.toCharArray();

        String ds = st.hasMoreElements() ? st.nextToken() : "a";
        while (ds.length() < cols.length) {
            ds = ds.concat(ds.substring(0, 1));
        }
        dir = ds.toCharArray();
    }

    @Override
    public DataTable tableOp(final DataTable result) {
        result.sort(new Comparator<Bundle>() {
            // TODO temp hack b/c table appends are BROKEN ATM WRT table.getFormat()
//          private final BundleField columns[] = new BundleColumnBinder(result, cols).getFields();
            private BundleField[] columns;

            public int compare(Bundle o1, Bundle o2) {
                if (columns == null) {
                    columns = new BundleColumnBinder(o1, cols).getFields();
                }
                int delta = 0;
                for (int i = 0; i < columns.length; i++) {
                    BundleField col = columns[i];
                    if (delta == 0) {
                        switch (type[i]) {
                            case 'i': // int
                            case 'l': // long
                            case 'n': // legacy "number"
                                delta = longCompare(o1.getValue(col), o2.getValue(col));
                                break;
                            case 'd': // double
                            case 'f': // float
                                delta = doubleCompare(o1.getValue(col), o2.getValue(col));
                                break;
                            case 's': // string
                            default:
                                delta = stringCompare(o1.getValue(col), o2.getValue(col));
                                break;
                        }

                        if (dir[i] == 'd') {
                            delta = -delta;
                        }
                    } else {
                        break;
                    }
                }
                return delta;
            }
        });
        return result;
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private int longCompare(ValueObject s1, ValueObject s2) {
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return Long.compare(ValueUtil.asNumberOrParseLong(s1, 10).asLong().getLong(),
                ValueUtil.asNumberOrParseLong(s2, 10).asLong().getLong());
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private int doubleCompare(ValueObject s1, ValueObject s2) {
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return Double.compare(ValueUtil.asNumberOrParseDouble(s1).asDouble().getDouble(),
                ValueUtil.asNumberOrParseDouble(s2).asDouble().getDouble());
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private int stringCompare(ValueObject s1, ValueObject s2) {
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return s1.toString().compareTo(s2.toString());
    }
}

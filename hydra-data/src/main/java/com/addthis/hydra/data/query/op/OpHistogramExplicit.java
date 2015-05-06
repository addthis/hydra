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

import java.util.Arrays;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">builds a histogram with explicit boundaries</span>.
 * <p>The syntax of the operation is "histo=[column],val1,val2,val3,etc. The values determine
 * the boundary points for the histogram. The result of this operation is a table with two columns.
 * Column 0 stores the lower bound of the bucket and column 1 stores the number of elements
 * within the bucket.
 *
 * @user-reference
 * @hydra-name histo2
 */
public class OpHistogramExplicit extends AbstractRowOp {

    private enum Mode {
        FLOAT, INTEGER
    }

    private final int column;
    private final int frequency;
    private final Mode mode;
    private final Number[] keys;
    private final long[] counts;

    /**
     * usage: column,val1,val2,val3,etc
     * <p/>
     * column defines the column source for the bucket value.
     * values define the boundaries of the histogram. Optionally
     * you can specify column1:column2 which will accept sources from
     * column1 and the frequency of each item from column2.
     *
     * @param args
     */
    public OpHistogramExplicit(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        int columns = args.indexOf(',');
        if (columns == -1) {
            throw new RuntimeException("syntax error two components not detected");
        }
        String columnsDefinition = args.substring(0, columns);
        int colonPos = columnsDefinition.indexOf(':');
        if (colonPos > -1) {
            frequency = Integer.parseInt(columnsDefinition.substring(colonPos + 1));
            columnsDefinition = columnsDefinition.substring(0, colonPos);
        } else {
            frequency = -1;
        }
        column = Integer.parseInt(columnsDefinition);
        String positions = args.substring(columns + 1);
        int fposition = positions.indexOf('.');
        if (fposition == -1) {
            mode = Mode.INTEGER;
            int[] v = csvToInts(positions);
            if (v.length < 2) {
                throw new RuntimeException("at least two boundary points required");
            }
            Arrays.sort(v);
            keys = new Number[v.length + 1];
            counts = new long[v.length + 1];
            keys[0] = Integer.MIN_VALUE;
            for (int i = 0; i < v.length; i++) {
                keys[i + 1] = v[i];
            }
        } else {
            mode = Mode.FLOAT;
            float[] v = csvToFloats(positions);
            if (v.length < 2) {
                throw new RuntimeException("at least two boundary points required");
            }
            Arrays.sort(v);
            keys = new Number[v.length + 1];
            counts = new long[v.length + 1];
            keys[0] = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < v.length; i++) {
                keys[i + 1] = v[i];
            }
        }
    }

    BundleColumnBinder binder;
    Bundle rowFactory;

    @Override
    public Bundle rowOp(Bundle row) {
        if (binder == null) {
            binder = getSourceColumnBinder(row);
            rowFactory = row.createBundle();
        }
        Numeric value = ValueUtil.asNumberOrParse(binder.getColumn(row, column));
        long increment;
        if (frequency >= 0) {
            Numeric incNumeric = ValueUtil.asNumberOrParseLong(binder.getColumn(row, frequency), 10);
            increment = (incNumeric != null) ? incNumeric.asLong().asNative() : 1;
        } else {
            increment = 1;
        }
        if (mode == Mode.FLOAT) {
            float target = (float) value.asDouble().getDouble();
            int position = Arrays.binarySearch(keys, target);
            if (position < 0) position = ~position - 1;
            counts[position] += increment;
        } else {
            int target = (int) value.asLong().getLong();
            int position = Arrays.binarySearch(keys, target);
            if (position < 0) position = ~position - 1;
            counts[position] += increment;
        }
        return null;
    }

    @Override
    public void sendComplete() {
        for(int i = 0; i < keys.length; i++) {
            if (opPromise.isDone()) {
                break;
            } else {
                Bundle row = rowFactory.createBundle();
                if (mode == Mode.FLOAT) {
                    binder.appendColumn(row, ValueFactory.create(keys[i].floatValue()));
                } else {
                    binder.appendColumn(row, ValueFactory.create(keys[i].intValue()));
                }
                binder.appendColumn(row, ValueFactory.create(counts[i]));
                getNext().send(row);
            }
        }
        super.sendComplete();
    }
}

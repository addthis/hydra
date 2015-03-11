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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueCustom;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueSimple;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.hydra.data.query.AbstractQueryOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">pivots a column by row and column keys</span>.
 * <p/>
 * <p>The syntax is pivot=[rowkeys],[colkeys],[colval],[colop],[rowop],[sort]. [rowkeys] is a sequence
 * of one or more row numbers delimited by colon characters. [colkeys] is a sequence of one or more
 * column numbers delimited by colon characters. [colval] is a column number. [colop] and
 * [rowop] are one of "max", "min", or "sum". [sort] is either "a" for ascending
 * or "d" for descending.
 * <p/>
 * <p>This operation transforms the entire table into a new output table. The values of the
 * output table are derived from the values that are in the [colval] column. The rows of the new
 * table are determined by [rowkeys] and the columns of the new table determined by [colkeys].
 * If two or more rows and/or columns are specified then the resulting rows or columns will
 * be the <a href="http://en.wikipedia.org/wiki/Cartesian_product">cartesian product</a> of
 * the specified values.</p>
 * <p/>
 * <p>In the output table the contents of row <i>i</i> and column <i>j</i> is the result
 * of the [colop] operation performed on all values from the input column that reside
 * in row <i>i</i> and column <i>j</i> of the original table. The output table will have
 * an additional column appended at the end. The contents of the additional column at row
 * <i>i</i> is the result of the [rowop] operation performed on all values from the input
 * column that reside in row <i>i</i>.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * 0 A 3
 * 1 A 1
 * 1 B 2
 * 0 A 5
 *
 * pivot=1,0,2,min,sum,d
 *
 *   0 1
 * A 3 1 9
 * B 0 2 2
 * </pre>
 *
 * @user-reference
 * @hydra-name pivot
 */
public class OpPivot extends AbstractQueryOp {

    public static final PivotMarkMin MIN = new PivotMarkMin();
    public static final PivotMarkMin MAX = new PivotMarkMax();
    public static final ValueLong ZERO = ValueFactory.create(0);

    private static enum PivotOp {
        SUM, MIN, MAX, AVG,
    }

    private static enum SortOp {
        LABEL_ASC, LABEL_DES, SUM_ASC, SUM_DES
    }

    private BundleColumnBinder rowbinder;
    private BundleColumnBinder colbinder;
    private BundleField        cellField;
    private BundleField        labelCol;
    private BundleField        sumCol;
    private PivotOp cellop = PivotOp.SUM;
    private PivotOp   rowop;
    private PivotOp   colop;
    private SortOp    sortop;
    private DataTable output;

    private final LinkedHashMap<String, BundleField> outCellField = new LinkedHashMap<>();
    private final SortedMap<String, Bundle>          pivot        = new TreeMap<>();

    private final DataTableFactory          tableFactory;
    private final String[]                  rowkeys;
    private final String[]                  colkeys;
    private final String                    cellkey;
    private final ChannelProgressivePromise queryPromise;

    public OpPivot(DataTableFactory tableFactory,
                   String args,
                   ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.tableFactory = tableFactory;
        this.output = tableFactory.createTable(0);
        this.queryPromise = queryPromise;
        String[] parg = LessStrings.splitArray(args, ",");
        rowkeys = LessStrings.splitArray(parg[0], ":");
        colkeys = LessStrings.splitArray(parg[1], ":");
        cellkey = parg[2];
        if (parg.length >= 4) {
            List<PivotOp> pop = new LinkedList<>();
            for (int i = 3; i < parg.length; i++) {
                switch (parg[i]) {
                    case "sum":
                        pop.add(PivotOp.SUM);
                        break;
                    case "min":
                        pop.add(PivotOp.MIN);
                        break;
                    case "max":
                        pop.add(PivotOp.MAX);
                        break;
                    case "avg":
                        pop.add(PivotOp.AVG);
                        break;
                    case "a":
                    case "sa":
                        sortop = SortOp.SUM_ASC;
                        break;
                    case "d":
                    case "sd":
                        sortop = SortOp.SUM_DES;
                        break;
                    case "la":
                        sortop = SortOp.LABEL_ASC;
                        break;
                    case "ld":
                        sortop = SortOp.LABEL_DES;
                        break;
                    case "":
                    case "null":
                    case "-":
                        pop.add(null);
                        break;
                }
            }
            cellop = pop.remove(0);
            rowop = pop.size() > 0 ? pop.remove(0) : null;
            colop = pop.size() > 0 ? pop.remove(0) : null;
        }
    }

    private Numeric doOp(PivotOp op, Numeric accum, Numeric cell) {
        if (accum == null || cell == null) {
            //System.out.println("accum = " + accum + "  cell = " + cell);
            return accum != null ? accum : cell;
        }
        if (op != null) {
            switch (op) {
                case SUM:
                    return accum.sum(cell);
                case MIN:
                    return accum.min(cell);
                case MAX:
                    return accum.max(cell);
                case AVG:
                    if (accum.getClass() != PivotAvg.class) {
                        accum = new PivotAvg(accum);
                    }
                    return accum.sum(cell);
            }
        }
        return accum;
    }

    @Override
    public void send(Bundle row) {
        if (rowbinder == null) {
            rowbinder = new BundleColumnBinder(row, rowkeys);
            colbinder = new BundleColumnBinder(row, colkeys);
            cellField = row.getFormat().getField(cellkey);
            labelCol = output.getFormat().getField("__row__");
        }
        /** generate column key or create if missing */
        String colkey = "";
        for (BundleField colfield : colbinder.getFields()) {
            colkey = colkey.concat(row.getValue(colfield).toString());
        }
        BundleField pivotCell = outCellField.get(colkey);
        if (pivotCell == null) {
            pivotCell = output.getFormat().getField(colkey);
            outCellField.put(colkey, pivotCell);
        }
        /** generate row key and fetch row */
        String rowkey = "";
        for (BundleField rowfield : rowbinder.getFields()) {
            rowkey = rowkey.concat(row.getValue(rowfield).toString());
        }
        Bundle pivotrow = pivot.get(rowkey);
        /** fill new row or append nulls to a short row */
        if (pivotrow == null) {
            pivotrow = output.createBundle();
            pivotrow.setValue(labelCol, ValueFactory.create(rowkey));
            pivot.put(rowkey, pivotrow);
        }
        /** fetch column cell from pivot and matching column cell from row */
        ValueObject inputValue = row.getValue(cellField);
        ValueObject pivotValue = pivotrow.getValue(pivotCell);
        if (pivotValue == null) {
            pivotValue = inputValue;
        } else {
            pivotValue = doOp(cellop, OpGather.num(pivotValue), OpGather.num(inputValue));
        }
        pivotrow.setValue(pivotCell, pivotValue);
    }

    @Override
    public void sendComplete() {
        sumCol = output.getFormat().getField("__sum__");
        // create and send pivot header
        ListBundle header = (ListBundle) output.createBundle();
        for (Entry<String, BundleField> e : outCellField.entrySet()) {
            header.setValue(e.getValue(), ValueFactory.create(e.getKey()));
        }
        // emit pivot rows
        Bundle footer = output.createBundle();
        for (Entry<String, Bundle> ent : pivot.entrySet()) {
            Bundle row = ent.getValue();
            // do rowop and/or colop if present
            if (rowop != null || colop != null) {
                Numeric rowaccum = null;
                Numeric colaccum;
                for (BundleField col : row.getFormat()) {
                    if (col == labelCol || col == sumCol) {
                        continue;
                    }
                    ValueObject cell = row.getValue(col);
                    if (cell == null) {
                        row.setValue(col, ZERO);
                        continue;
                    }
                    if (colop != null) {
                        colaccum = OpGather.num(footer.getValue(col));
                        if (colaccum == null) {
                            footer.setValue(col, cell);
                        } else {
                            footer.setValue(col, doOp(colop, colaccum, OpGather.num(cell)));
                        }
                    }
                    if (rowop != null) {
                        if (rowaccum == null) {
                            rowaccum = OpGather.num(cell);
                        } else {
                            rowaccum = doOp(rowop, rowaccum, OpGather.num(cell));
                        }
                    }
                }
                if (rowaccum != null) {
                    if (rowop == PivotOp.AVG && rowaccum.getClass() == PivotAvg.class) {
                        /* see the code -- zero is ignored */
                        rowaccum = rowaccum.avg(0);
                    }
                    row.setValue(sumCol, rowaccum);
                }
            }
            output.append(row);
        }
        if (colop != null) {
            if (colop == PivotOp.AVG) {
                for (BundleField col : footer.getFormat()) {
                    if (col == labelCol || col == sumCol) {
                        continue;
                    }
                    /* see the code -- zero is ignored */
                    footer.setValue(col, OpGather.num(footer.getValue(col)).avg(0));
                }
            }
        }
        // sort table
        if (sortop != null) {
            String sortstr = null;
            switch (sortop) {
                case LABEL_ASC:
                    sortstr = labelCol.getName() + ":s:a";
                    break;
                case LABEL_DES:
                    sortstr = labelCol.getName() + ":s:d";
                    break;
                case SUM_ASC:
                    sortstr = sumCol.getName() + ":n:a";
                    break;
                case SUM_DES:
                    sortstr = sumCol.getName() + ":n:d";
                    break;
            }
            OpSort sort = new OpSort(tableFactory, sortstr, queryPromise);
            sort.sendTable(output);
            output = sort.getTable();
        }
        // prepend header
        if (header.getFormat().getFieldCount() > 0) {
            output.insert(0, header);
        }
        // append footer
        if (colop != null) {
            output.append(footer);
        }
        getNext().sendTable(output);
    }

    /**
     * special markers to help sorting pivots
     */
    public static class PivotMarkMax extends PivotMarkMin {

        @Override
        public PivotMarkMax max(Numeric val) {
            return this;
        }

        @Override
        public Numeric min(Numeric val) {
            return val;
        }
    }

    /**
     * special markers to help sorting pivots.
     * must be ValueCustom instead of ValueObect
     * so that it survives serialization
     */
    public static class PivotMarkMin implements ValueCustom<Long>, Numeric {

        @Override
        public String toString() {
            return "";
        }

        @Override
        public PivotMarkMin avg(int count) {
            return this;
        }

        @Override
        public Numeric diff(Numeric val) {
            return val;
        }

        @Override
        public Numeric max(Numeric val) {
            return val;
        }

        @Override
        public Numeric min(Numeric val) {
            return this;
        }

        @Override
        public Numeric sum(Numeric val) {
            return val;
        }

        public Long toLong() {
            return Long.MAX_VALUE;
        }

        @Override
        public ValueLong asLong() {
            return ValueFactory.create(toLong());
        }

        @Override
        public ValueString asString() {
            return ValueFactory.create("");
        }

        @Override
        public TYPE getObjectType() {
            return TYPE.CUSTOM;
        }

        @Override public Long asNative() {
            return Long.MAX_VALUE;
        }

        @Override
        public ValueBytes asBytes() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueArray asArray() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public Numeric asNumeric() throws ValueTranslationException {
            return this;
        }

        @Override
        public ValueDouble asDouble() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueCustom asCustom() throws ValueTranslationException {
            return this;
        }

        @Override
        public ValueMap asMap() throws ValueTranslationException {
            return ValueFactory.createMap();
        }

        @Override
        public void setValues(ValueMap map) {
        }

        @Override
        public ValueSimple asSimple() {
            return asLong();
        }
    }

    /**
     * for doing averages
     */
    private static class PivotAvg implements Numeric {

        private Numeric orig;
        private int ops;

        PivotAvg(ValueObject orig) {
            this.orig = ValueUtil.asNumberOrParseLong(orig, 10);
            this.ops = 1;
        }

        @Override
        public String toString() {
            return orig.toString();
        }

        @Override
        public Numeric avg(int count) {
            return orig.avg(ops);
        }

        @Override
        public PivotAvg diff(Numeric val) {
            ops++;
            orig = orig.diff(val);
            return this;
        }

        @Override
        public PivotAvg max(Numeric val) {
            ops++;
            orig = orig.max(val);
            return this;
        }

        @Override
        public PivotAvg min(Numeric val) {
            ops++;
            orig = orig.min(val);
            return this;
        }

        @Override
        public PivotAvg sum(Numeric val) {
            ops++;
            orig = orig.sum(val);
            return this;
        }

        @Override
        public ValueDouble asDouble() {
            return orig.asDouble();
        }

        @Override
        public ValueLong asLong() {
            return orig.asLong();
        }

        @Override
        public ValueString asString() {
            return orig.asString();
        }

        @Override
        public TYPE getObjectType() {
            return orig.getObjectType();
        }

        @Override public Numeric asNative() {
            return orig;
        }

        @Override
        public ValueBytes asBytes() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueArray asArray() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public ValueMap asMap() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override
        public PivotAvg asNumeric() throws ValueTranslationException {
            return this;
        }

        @Override
        public ValueCustom asCustom() throws ValueTranslationException {
            throw new ValueTranslationException();
        }
    }
}

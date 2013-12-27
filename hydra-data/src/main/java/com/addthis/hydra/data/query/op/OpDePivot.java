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
import java.util.LinkedList;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractBufferOp;
import com.addthis.hydra.data.query.QueryOpProcessor;


/**
 * <p>This query operation <span class="hydra-summary">unwinds a pivot so that it can
 * be merged with another pivot</span>.
 *
 * @user-reference
 * @hydra-name depivot
 */
public class OpDePivot extends AbstractBufferOp {

    private static final Class<?> ex1 = OpPivot.PivotMarkMax.class;
    private static final Class<?> ex2 = OpPivot.PivotMarkMin.class;

    private Integer rowcol;
    private Integer colcol;
    private Integer sumcol;
    private Integer valcol;
    private int cols = 0;
    private final LinkedList<ValueObject> columns = new LinkedList<>();
    private final ListBundleFormat format = new ListBundleFormat();
    private final BundleField rowField;
    private final BundleField colField;
    private final BundleField valField;
    private final BundleField sumField;

    public OpDePivot(QueryOpProcessor processor) {
        this(processor, null);
    }

    public OpDePivot(QueryOpProcessor processor, String args) {
        if (args != null && args.length() > 0) {
            String ops[] = Strings.splitArray(args, ",");
            for (String op : ops) {
                if (op.equals("row")) {
                    rowcol = cols++;
                } else if (op.equals("col")) {
                    colcol = cols++;
                } else if (op.equals("sum")) {
                    sumcol = cols++;
                } else if (op.equals("val")) {
                    valcol = cols++;
                }
            }
        } else {
            rowcol = 0;
            colcol = 1;
            valcol = 2;
            cols = 3;
        }
        rowField = rowcol != null ? format.getField(Integer.toString(rowcol)) : null;
        colField = colcol != null ? format.getField(Integer.toString(colcol)) : null;
        valField = valcol != null ? format.getField(Integer.toString(valcol)) : null;
        sumField = sumcol != null ? format.getField(Integer.toString(sumcol)) : null;
    }

    @Override
    public List<Bundle> finish() {
        return null;
    }

    @Override
    public List<Bundle> next(Bundle row) {
        BundleColumnBinder binder = getSourceColumnBinder(row);
        Class<?> cellClass = binder.getColumn(row, 0).getClass();
        if (cellClass == ex1 || cellClass == ex2) {
            columns.clear();
            for (int i = 1; i < row.getCount(); i++) {
                ValueObject cell = binder.getColumn(row, i);
                cellClass = cell.getClass();
                if (cellClass == ex1 || cellClass == ex2) {
                    break;
                }

                columns.add(cell);
            }
            return null;
        }
        List<Bundle> newrows = new ArrayList<>(columns.size());
        ValueObject rowKey = binder.getColumn(row, 0);
        int pos = 1;
        for (ValueObject col : columns) {
            Bundle newrow = new ListBundle(format);
            if (rowcol != null) {
                newrow.setValue(rowField, rowKey);
            }
            if (colcol != null) {
                newrow.setValue(colField, col);
            }
            if (valcol != null && row.getCount() > pos) {
                newrow.setValue(valField, binder.getColumn(row, pos));
            }
            if (sumcol != null) {
                newrow.setValue(sumField, binder.getColumn(row, row.getCount() - 1));
            }
            newrows.add(newrow);
            pos++;
        }
        return newrows;
    }

}

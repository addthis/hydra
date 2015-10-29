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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractQueryOp;

import com.google.common.base.Strings;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">unwinds a pivot so that it can
 * be merged with another pivot</span>.
 *
 * @user-reference
 * @hydra-name depivot
 */
public class OpDePivot extends AbstractQueryOp {

    private Integer rowcol;
    private Integer colcol;
    private Integer extraCol;
    private Integer valcol;
    private int cols = 0;
    private Deque<ValueObject> columns;
    private final ListBundleFormat format;
    private final BundleField rowField;
    private final BundleField colField;
    private final BundleField valField;
    private final BundleField extraField;

    public OpDePivot(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.format = new ListBundleFormat();
        if (!Strings.isNullOrEmpty(args)) {
            String[] ops = LessStrings.splitArray(args, ",");
            for (String op : ops) {
                switch (op) {
                    case "row":
                        rowcol = cols++;
                        break;
                    case "col":
                        colcol = cols++;
                        break;
                    case "val":
                        valcol = cols++;
                        break;
                    case "extra":
                        extraCol = cols++;
                        break;
                    default:
                        throw new IllegalArgumentException(op + "is not a recognized pivot component");
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
        extraField = extraCol != null ? format.getField(Integer.toString(extraCol)) : null;
    }

    @Override
    public void send(Bundle row) {
        ValueArray rowView = row.asValueArray();
        ValueObject rowKey = rowView.get(0);
        if (columns == null) {
            columns = new ArrayDeque<>(rowView.size() - 1);
            for (int i = 1; i < rowView.size(); i++) {
                ValueObject cell = rowView.get(i);
                if (cell != null) {
                    columns.add(cell);
                }
            }
            return;
        }
        List<Bundle> newrows = new ArrayList<>(columns.size());
        int pos = 1;
        for (ValueObject col : columns) {
            Bundle newrow = format.createBundle();
            if (rowcol != null) {
                newrow.setValue(rowField, rowKey);
            }
            if (colcol != null) {
                newrow.setValue(colField, col);
            }
            if (valcol != null && row.getCount() > pos) {
                newrow.setValue(valField, rowView.get(pos));
            }
            if (extraCol != null) {
                newrow.setValue(extraField, rowView.get(rowView.size() - 1));
            }
            newrows.add(newrow);
            pos++;
        }
        getNext().send(newrows);
    }

    @Override
    public void sendComplete() {
        getNext().sendComplete();
    }
}

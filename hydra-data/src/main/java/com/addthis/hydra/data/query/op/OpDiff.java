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
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueString;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">calculates the delta between two values</span>.
 * <p/>
 * <p>The syntax is delta=[one or more parameters] where parameter is either "k" for key, "a"
 * for alternating key,"d" for difference, or "D" for negative difference. The number
 * of columns in the output table is equal to the number of parameters.
 * <ul>
 * <li>a = alternating column key
 * <li>k = primary key column
 * <li>d = difference column
 * <li>D = negative difference column
 * </ul>
 *
 * @hydra-name diff
 */
public class OpDiff extends AbstractTableOp {

    public static enum ColumnType {
        ALTKEY, KEY, DIFF, NEGDIFF
    }

    private static enum DiffOp {
        ADD, DROP, DIFF
    }

    private static final ValueString plusString = ValueFactory.create("+");
    private static final ValueString minusString = ValueFactory.create("-");

    private ColumnType[] type;

    public OpDiff(DataTableFactory tableFactory, String args, ChannelProgressivePromise queryPromise) {
        super(tableFactory, queryPromise);
        type = new ColumnType[args.length()];
        for (int i = 0; i < args.length(); i++) {
            switch (args.charAt(i)) {
                // alternating key
                case 'a':
                    type[i] = ColumnType.ALTKEY;
                    break;
                // part of compound key
                case 'k':
                    type[i] = ColumnType.KEY;
                    break;
                // difference
                case 'd':
                    type[i] = ColumnType.DIFF;
                    break;
                // -difference
                case 'D':
                    type[i] = ColumnType.NEGDIFF;
                    break;
            }
        }
    }

    public OpDiff(DataTableFactory processor, ColumnType[] type, ChannelProgressivePromise queryPromise) {
        super(processor, queryPromise);
        this.type = type;
    }

    @Override
    public DataTable tableOp(DataTable result) {
        BundleColumnBinder binder = getSourceColumnBinder(result);
        String[] altkeys = new String[2];
        String lastkey = null;
        // find alternating keys
        for (Bundle line : result) {
            String key = "";
            String altkey = "";
            for (int j = 0; j < type.length; j++) {
                switch (type[j]) {
                    case KEY:
                        key += ValueUtil.asNativeString(binder.getColumn(line, j));
                        break;
                    case ALTKEY:
                        altkey = ValueUtil.asNativeString(binder.getColumn(line, j));
                        break;
                }
            }
            if (lastkey == null || !lastkey.equals(key)) {
                lastkey = key;
                altkeys[0] = altkey;
            } else {
                altkeys[1] = altkey;
                break;
            }
        }
        // do diff
        Bundle lastline = null;
        lastkey = "";
        boolean first = true;
        DataTable rez = createTable(result.size() / 2 + 1);
        for (Bundle line : result) {
            String key = "";
            String altkey = "";
            // compute key and alt key
            for (int j = 0; j < type.length; j++) {
                switch (type[j]) {
                    case KEY:
                        key = key.concat(ValueUtil.asNativeString(binder.getColumn(line, j)));
                        break;
                    case ALTKEY:
                        altkey = ValueUtil.asNativeString(binder.getColumn(line, j));
                        break;
                }
            }
            DiffOp doop;
            if (first) {
                if (altkey.equals(altkeys[0])) {
                    // normal sequence: do calcs and reset
                    lastline = line;
                    lastkey = key;
                    first = false;
                    continue;
                } else {
                    // missing first alt key means new key, do calcs and reset
                    doop = DiffOp.ADD;
                }
            } else {
                if (key.equals(lastkey)) {
                    // normal sequence: do calcs and reset
                    doop = DiffOp.DIFF;
                } else {
                    // means dropped key, use last line/key values and continue
                    // as if it was first
                    doop = DiffOp.DROP;
                }
            }
            Bundle add = rez.createBundle();
            switch (doop) {
                case ADD:
                    for (int j = 0; j < type.length; j++) {
                        switch (type[j]) {
                            case ALTKEY:
                                binder.appendColumn(add, plusString);
                                break;
                            case DIFF:
                                binder.appendColumn(add, binder.getColumn(line, j));
                                break;
                            case NEGDIFF:
                                binder.appendColumn(add, ZERO.diff(OpGather.num(binder.getColumn(line, j))));
                                break;
                            default:
                                binder.appendColumn(add, binder.getColumn(line, j));
                                break;
                        }
                    }
                    rez.append(add);
                    break;
                case DROP:
                    for (int j = 0; j < type.length; j++) {
                        switch (type[j]) {
                            case ALTKEY:
                                binder.appendColumn(add, minusString);
                                break;
                            case KEY:
                                binder.appendColumn(add, ValueFactory.create(lastkey));
                                break;
                            case DIFF:
                                binder.appendColumn(add, ZERO.diff(OpGather.num(binder.getColumn(lastline, j))));
                                break;
                            case NEGDIFF:
                                binder.appendColumn(add, binder.getColumn(lastline, j));
                                break;
                            default:
                                binder.appendColumn(add, binder.getColumn(lastline, j));
                                break;
                        }
                    }
                    rez.append(add);
                    lastkey = key;
                    lastline = line;
                    break;
                case DIFF:
                    for (int j = 0; j < type.length; j++) {
                        switch (type[j]) {
                            case ALTKEY:
                                binder.appendColumn(add, EMPTY_STRING);
                                break;
                            case DIFF:
                                binder.appendColumn(add,
                                        OpGather.num(binder.getColumn(line, j)).diff(
                                                OpGather.num(binder.getColumn(lastline, j)))
                                );
                                break;
                            case NEGDIFF:
                                binder.appendColumn(add,
                                        OpGather.num(binder.getColumn(lastline, j)).diff(
                                                OpGather.num(binder.getColumn(line, j)))
                                );
                                break;
                            default:
                                binder.appendColumn(add, binder.getColumn(line, j));
                                break;
                        }
                    }
                    rez.append(add);
                    first = true;
                    lastline = null;
                    break;
            }
        }
        return rez;
    }
}

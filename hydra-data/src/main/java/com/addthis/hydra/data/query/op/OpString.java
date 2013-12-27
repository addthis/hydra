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
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;


/**
 * <p>This query operation <span class="hydra-summary">performs postfix (RPN)
 * calculator string operations</span>.
 * <p/>
 * <p>This filter uses a calculation stack to perform string operations. The stack is initially empty
 * and values can be pushed or popped from the top of the stack. String operations can be performed
 * on the element(s) at the top of the stack.
 * <pre style="font-family:courier;font-size:15px">
 * num=OP,OP,OP,...
 * <p/>
 * OP         := LOAD_OP | CALC_OP
 * LOAD_OP    :=  LOAD_COL | LOAD_VAL
 * LOAD_COL   := ("c" | "col" ) INT
 * LOAD_VAL   := ("v" | "val" ) STRING
 * CALC_OP    := [see table for operations]
 * INT        := DIGIT+
 * DIGIT      := ["0" - "9"]</pre>
 * <p/>
 * <p>The following operations below are available. Each operation proceeds in three steps:
 * (1) Zero or more values are popped off the stack, (2) an operation is performed on these values,
 * and (3) zero or more values are pushed onto the stack. For convenience the following table
 * labels the first element popped off the stack as "a" (the topmost element), the second
 * element as "b", etc. The variables "X","Y","Z", etc. represent numbers in the command sequence.
 * <p/>
 * <table width="80%" class="num">
 * <tr>
 * <th width="15%">name</th>
 * <th width="10%">pop count</th>
 * <th width="65%">operation</th>
 * <th width="10%">push count</th>
 * </tr>
 * <tr>
 * <td>"cat" or "+"</td>
 * <td>2</td>
 * <td>concatenate two strings</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"range" or ":"</td>
 * <td>3</td>
 * <td>take the range of string c from offset a to offset b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"split" or "/"</td>
 * <td>3</td>
 * <td>split string c using separator b and push element number a from the result</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"set" or "="</td>
 * <td>2</td>
 * <td>set column a to string b</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"eq" or "=="</td>
 * <td>2</td>
 * <td>if string equality is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"neq" or "!="</td>
 * <td>2</td>
 * <td>if string equality is true then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"cX"</td>
 * <td>0</td>
 * <td>push the string in column X onto the stack</td>
 * <td>len(args)</td>
 * </tr>
 * <tr>
 * <td>"vX"</td>
 * <td>0</td>
 * <td>push the string X onto the stack</td>
 * <td>1</td>
 * </tr>
 * </table>
 * <p/>
 * <p>Example 1:</p>
 * <pre>
 * A 1 art
 * B 2 bot
 * C 3 cog
 * D 4 din
 *
 * str=c0,c1,cat,v2,set
 *
 * A 1 A1
 * B 2 B2
 * C 3 C3
 * D 4 D4
 * </pre>
 * <p/>
 * <p>Example 2:</p>
 * <pre>
 * A 1|||a art
 * B 2|||b bot
 * C 3|||c cog
 * D 4|||d din
 *
 * str=c1,v|||,v0,split,v2,set,
 *
 * A 1 1
 * B 2 2
 * C 3 3
 * D 4 4
 * </pre>
 *
 * @user-reference
 * @hydra-name str
 */
public class OpString extends AbstractRowOp {

    private static final int OP_CAT = -1;
    private static final int OP_RANGE = -2;
    private static final int OP_SPLIT = -3;
    private static final int OP_COLVAL = -4;
    private static final int OP_CONST = -5;
    private static final int OP_SET = -6;
    private static final int OP_EQUAL = -7;
    private static final int OP_NOTEQUAL = -8;

    private final List<StringOp> ops;

    public OpString(String args) {
        String op[] = Strings.splitArray(args, ",");
        ops = new ArrayList<>(op.length);
        for (String o : op) {
            if (o.equals("+") || o.equals("cat")) {
                ops.add(new StringOp(OP_CAT, null));
            } else if (o.equals(":") || o.equals("range")) {
                ops.add(new StringOp(OP_RANGE, null));
            } else if (o.equals("/") || o.equals("split")) {
                ops.add(new StringOp(OP_SPLIT, null));
            } else if (o.equals("=") || o.equals("set")) {
                ops.add(new StringOp(OP_SET, null));
            } else if (o.equals("==") || o.equals("eq")) {
                ops.add(new StringOp(OP_EQUAL, null));
            } else if (o.equals("!=") || o.equals("neq")) {
                ops.add(new StringOp(OP_NOTEQUAL, null));
            } else {
                if (o.startsWith("c")) {
                    ops.add(new StringOp(OP_COLVAL, ValueFactory.create(o.substring(1))));
                } else if (o.startsWith("v")) {
                    ops.add(new StringOp(OP_CONST, ValueFactory.create(o.substring(1))));
                }
            }
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        LinkedList<ValueObject> stack = new LinkedList<>();
        long maxcol = row.getCount() - 1;
        for (StringOp op : ops) {
            switch (op.type) {
                case OP_CAT:
                    ValueObject rval = stack.pop();
                    stack.push(ValueFactory.create(stack.pop().toString().concat(rval.toString())));
                    break;
                case OP_RANGE:
                    int last = stack.pop().asLong().getLong().intValue();
                    int first = stack.pop().asLong().getLong().intValue();
                    String str = stack.pop().toString();
                    int len = str.length();
                    if (len == 0) {
                        stack.push(ValueFactory.create(""));
                        break;
                    }
                    if (first < 0) {
                        first = len + first;
                    }
                    if (last < 0) {
                        last = len + last;
                    }
                    if (first > len) {
                        first = len - 1;
                    }
                    if (last > len) {
                        last = len;
                    }
                    stack.push(ValueFactory.create(str.substring(first, last)));
                    break;
                case OP_SPLIT:
                    int pos = stack.pop().asLong().getLong().intValue();
                    String sep = stack.pop().toString();
                    str = stack.pop().toString();
                    String seg[] = Strings.splitArray(str, sep);
                    stack.push(ValueFactory.create(seg[pos]));
                    break;
                case OP_COLVAL:
                    stack.push(getSourceColumnBinder(row).getColumn(row, op.val.asLong().getLong().intValue()));
                    break;
                case OP_CONST:
                    stack.push(op.val);
                    break;
                case OP_SET:
                    int col = stack.pop().asLong().getLong().intValue();
                    ValueObject val = stack.pop();
                    if (col < 0 || col > maxcol) {
                        getSourceColumnBinder(row).appendColumn(row, val);
                    } else {
                        getSourceColumnBinder(row).setColumn(row, col, val);
                    }
                    break;
                case OP_EQUAL:
                    if (!ValueUtil.isEqual(stack.pop(), stack.pop())) {
                        return null;
                    }
                    break;
                case OP_NOTEQUAL:
                    if (ValueUtil.isEqual(stack.pop(), stack.pop())) {
                        return null;
                    }
                    break;
                default:
                    break;
            }
        }
        return row;
    }

    /** */
    private class StringOp {

        private int type;
        private ValueObject val;

        StringOp(int type, ValueObject val) {
            this.type = type;
            this.val = val;
        }
    }
}

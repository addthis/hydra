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
package com.addthis.hydra.data.filter.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;

public class BundleCalculator {

    private static enum Operation {
        OP_ADD,
        OP_SUB,
        OP_DIV,
        OP_MULT,
        OP_REM,
        OP_SET,
        OP_COLVAL,
        OP_VAL,
        OP_SWAP,
        OP_GT,
        OP_LT,
        OP_GT_EQ,
        OP_LT_EQ,
        OP_EQ,
        OP_DUP,
        OP_LOG,
        OP_DMULT,
        OP_DDIV,
        OP_TOINT,
        OP_TOFLOAT,
        OP_SHIFTOUT,
        OP_SQRT,
        OP_DGT,
        OP_DLT,
        OP_DGT_EQ,
        OP_DLT_EQ,
        OP_DEQ,
        OP_MEAN,
        OP_VARIANCE,
        OP_MIN,
        OP_MAX,
        OP_MINIF,
        OP_MAXIF,
        OP_ABS
    }

    private List<MathOp> ops;
    private boolean diverr;
    private BundleColumnBinder sourceBinder;

    public BundleCalculator(String args) {
        String op[] = Strings.splitArray(args, ",");
        ops = new ArrayList<>(op.length);
        for (String o : op) {
            if (o.equals("+") || o.equals("add")) {
                ops.add(new MathOp(Operation.OP_ADD, null));
            } else if (o.equals("-") || o.equals("sub")) {
                ops.add(new MathOp(Operation.OP_SUB, null));
            } else if (o.equals("*") || o.equals("mult") || o.equals("prod")) {
                ops.add(new MathOp(Operation.OP_MULT, null));
            } else if (o.equals("dmult") || o.equals("dprod")) {
                ops.add(new MathOp(Operation.OP_DMULT, null));
            } else if (o.equals("/") || o.equals("div")) {
                ops.add(new MathOp(Operation.OP_DIV, null));
            } else if (o.equals("ddiv")) {
                ops.add(new MathOp(Operation.OP_DDIV, null));
            } else if (o.equals("log")) {
                ops.add(new MathOp(Operation.OP_LOG, null));
            } else if (o.equals("%") || o.equals("rem")) {
                ops.add(new MathOp(Operation.OP_REM, null));
            } else if (o.equals("=") || o.equals("s") || o.equals("set")) {
                ops.add(new MathOp(Operation.OP_SET, null));
            } else if (o.equals("x") || o.equals("swap")) {
                ops.add(new MathOp(Operation.OP_SWAP, null));
            } else if (o.equals("d") || o.equals("dup")) {
                ops.add(new MathOp(Operation.OP_DUP, null));
            } else if (o.equals(">") || o.equals("gt")) {
                ops.add(new MathOp(Operation.OP_GT, null));
            } else if (o.equals(">=") || o.equals("gteq")) {
                ops.add(new MathOp(Operation.OP_GT_EQ, null));
            } else if (o.equals("<") || o.equals("lt")) {
                ops.add(new MathOp(Operation.OP_LT, null));
            } else if (o.equals("<=") || o.equals("lteq")) {
                ops.add(new MathOp(Operation.OP_LT_EQ, null));
            } else if (o.equals("=") || o.equals("eq")) {
                ops.add(new MathOp(Operation.OP_EQ, null));
            } else if (o.equals("toi") || o.equals("toint")) {
                ops.add(new MathOp(Operation.OP_TOINT, null));
            } else if (o.equals("tof") || o.equals("tofloat")) {
                ops.add(new MathOp(Operation.OP_TOFLOAT, null));
            } else if (o.equals("out") || o.equals("shiftout")) {
                ops.add(new MathOp(Operation.OP_SHIFTOUT, null));
            } else if (o.equals("sqrt")) {
                ops.add(new MathOp(Operation.OP_SQRT, null));
            } else if (o.equals("diverr")) {
                diverr = true;
            } else if (o.equals(">>") || o.equals("dgt")) {
                ops.add(new MathOp(Operation.OP_DGT, null));
            } else if (o.equals(">>=") || o.equals("dgteq")) {
                ops.add(new MathOp(Operation.OP_DGT_EQ, null));
            } else if (o.equals("<<") || o.equals("dlt")) {
                ops.add(new MathOp(Operation.OP_DLT, null));
            } else if (o.equals("<<=") || o.equals("dlteq")) {
                ops.add(new MathOp(Operation.OP_DLT_EQ, null));
            } else if (o.equals("==") || o.equals("deq")) {
                ops.add(new MathOp(Operation.OP_DEQ, null));
            } else if (o.equals("min")) {
                ops.add(new MathOp(Operation.OP_MIN, null));
            } else if (o.equals("max")) {
                ops.add(new MathOp(Operation.OP_MAX, null));
            } else if (o.equals("minif")) {
                ops.add(new MathOp(Operation.OP_MINIF, null));
            } else if (o.equals("maxif")) {
                ops.add(new MathOp(Operation.OP_MAXIF, null));
            } else if (o.equals("abs")) {
                ops.add(new MathOp(Operation.OP_ABS, null));
            } else if (o.equals("mean")) {
                ops.add(new MathOp(Operation.OP_MEAN, null));
            } else if (o.equals("variance")) {
                ops.add(new MathOp(Operation.OP_VARIANCE, null));
            } else {
                if (o.startsWith("c")) {
                    String cols[] = Strings.splitArray(o.substring(1), ":");
                    for (String col : cols) {
                        ops.add(new MathOp(Operation.OP_COLVAL, ValueFactory.create(col)));
                    }
                } else if (o.startsWith("n")) {
                    String nums[] = Strings.splitArray(o.substring(1), ":");
                    for (String num : nums) {
                        if (num.indexOf(".") >= 0) {
                            ops.add(new MathOp(Operation.OP_VAL, ValueFactory.create(Double.parseDouble(num))));
                        } else {
                            ops.add(new MathOp(Operation.OP_VAL, ValueFactory.create(Long.parseLong(num))));
                        }
                    }
                } else if (o.startsWith("v")) {
                    ops.add(new MathOp(Operation.OP_VAL, ValueFactory.create(o.substring(1))));
                }
            }
        }
    }

    public Bundle calculate(Bundle line) {
        LinkedList<ValueNumber> stack = new LinkedList<ValueNumber>();
        long maxcol = line.getCount() - 1;
        for (MathOp op : ops) {
            switch (op.type) {
                case OP_ADD:
                    stack.push(stack.pop().sum(stack.pop()));
                    break;
                case OP_SUB:
                    ValueNumber v1 = stack.pop();
                    ValueNumber v2 = stack.pop();
                    stack.push(v2.diff(v1));
                    break;
                case OP_MULT:
                    stack.push(ValueFactory.create(stack.pop().asLong().getLong() * stack.pop().asLong().getLong()));
                    break;
                case OP_DMULT:
                    stack.push(ValueFactory.create(stack.pop().asDouble().getDouble() * stack.pop().asDouble().getDouble()));
                    break;
                case OP_DIV:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!diverr && v1.asLong().getLong() == 0) {
                        stack.push(ValueFactory.create(0));
                    } else {
                        stack.push(ValueFactory.create(v2.asLong().getLong() / v1.asLong().getLong()));
                    }
                    break;
                case OP_DDIV:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!diverr && v1.asDouble().getDouble() == 0d) {
                        stack.push(ValueFactory.create(0));
                    } else {
                        stack.push(ValueFactory.create(v2.asDouble().getDouble() / v1.asDouble().getDouble()));
                    }
                    break;
                case OP_REM:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!diverr && v1.asLong().getLong() == 0) {
                        stack.push(ValueFactory.create(0));
                    } else {
                        stack.push(ValueFactory.create(v2.asLong().getLong() % v1.asLong().getLong()));
                    }
                    break;
                case OP_LOG:
                    stack.push(ValueFactory.create(Math.log10(stack.pop().asDouble().getDouble())));
                    break;
                case OP_SQRT:
                    stack.push(ValueFactory.create(Math.sqrt(stack.pop().asDouble().getDouble())));
                    break;
                case OP_VAL:
                    stack.push(op.val.asNumber());
                    break;
                case OP_COLVAL:
                    stack.push(getSourceColumnBinder(line).getColumn(line, op.val.asLong().getLong().intValue()).asNumber());
                    break;
                case OP_DUP:
                    stack.push(stack.peek());
                    break;
                case OP_TOINT:
                    stack.push(ValueUtil.asNumberOrParseLong(stack.pop(), 10).asLong());
                    break;
                case OP_TOFLOAT:
                    stack.push(ValueUtil.asNumberOrParseDouble(stack.pop()));
                    break;
                case OP_DGT:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asDouble().getDouble() > v1.asDouble().getDouble())) {
                        return null;
                    }
                    break;
                case OP_DGT_EQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asDouble().getDouble() >= v1.asDouble().getDouble())) {
                        return null;
                    }
                    break;
                case OP_DLT:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asDouble().getDouble() < v1.asDouble().getDouble())) {
                        return null;
                    }
                    break;
                case OP_DLT_EQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asDouble().getDouble() <= v1.asDouble().getDouble())) {
                        return null;
                    }
                    break;
                case OP_DEQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asDouble().getDouble().equals(v1.asDouble().getDouble()))) {
                        return null;
                    }
                    break;
                case OP_GT:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asLong().getLong() > v1.asLong().getLong())) {
                        return null;
                    }
                    break;
                case OP_GT_EQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asLong().getLong() >= v1.asLong().getLong())) {
                        return null;
                    }
                    break;
                case OP_LT:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asLong().getLong() < v1.asLong().getLong())) {
                        return null;
                    }
                    break;
                case OP_LT_EQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asLong().getLong() <= v1.asLong().getLong())) {
                        return null;
                    }
                    break;
                case OP_EQ:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    if (!(v2.asLong().getLong().equals(v1.asLong().getLong()))) {
                        return null;
                    }
                    break;
                case OP_SWAP:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v1);
                    stack.push(v2);
                    break;
                case OP_SHIFTOUT:
                    getSourceColumnBinder(line).appendColumn(line, stack.pop());
                    break;
                case OP_SET:
                    int col = stack.pop().asLong().getLong().intValue();
                    ValueNumber val = stack.pop();
                    if (col < 0 || col > maxcol) {
                        getSourceColumnBinder(line).appendColumn(line, val);
                    } else {
                        getSourceColumnBinder(line).setColumn(line, col, val);
                    }
                    break;
                case OP_MIN:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v1.min(v2));
                    break;
                case OP_MAX:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v1.max(v2));
                    break;
                case OP_MINIF:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v1.max(v2).equals(v1) ? v1 : v2);
                    break;
                case OP_MAXIF:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v1.min(v2).equals(v1) ? v1 : v2);
                    break;
                case OP_MEAN: {
                    long count = 0;
                    double mean = 0.0;

                    while (!stack.isEmpty()) {
                        count++;
                        double num = stack.pop().asDouble().getDouble();
                        double delta = num - mean;
                        mean += delta / count;
                    }

                    stack.push(ValueFactory.create(mean));
                    break;
                }
                case OP_VARIANCE: {
                    long count = 0;
                    double mean = 0.0;
                    double m2 = 0.0;

                    while (!stack.isEmpty()) {
                        count++;
                        double num = stack.pop().asDouble().getDouble();
                        double delta = num - mean;
                        mean += delta / count;
                        m2 += delta * (num - mean);
                    }

                    if (count < 2) {
                        stack.push(ValueFactory.create(0.0));
                    } else {
                        double variance = m2 / count;
                        stack.push(ValueFactory.create(variance));
                    }

                    break;
                }
                case OP_ABS:
                    v1 = stack.pop();
                    stack.push(ValueFactory.create(Math.abs(v1.asDouble().getDouble())));
                default:
                    break;
            }
        }
        return line;
    }

    /** */
    private class MathOp {

        private
        @Nonnull
        Operation type;
        private
        @Nullable
        ValueObject val;

        MathOp(Operation type, ValueObject val) {
            this.type = type;
            this.val = val;
        }
    }

    /*
        Copied from AbstractQueryOp
     */
    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row) {
        return getSourceColumnBinder(row, null);
    }

    /*
        Copied from AbstractQueryOp
     */
    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row, String fields[]) {
        if (sourceBinder == null) {
            sourceBinder = new BundleColumnBinder(row, fields);
        }
        return sourceBinder;
    }
}

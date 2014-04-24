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

import java.text.NumberFormat;
import java.text.ParseException;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;

import com.google.common.primitives.Doubles;

public class BundleCalculator {

    private static final BundleCalculatorVector vector = BundleCalculatorVector.getSingleton();

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
        OP_ABS,
        OP_COLNAMEVAL,
        OP_VECTOR
    }

    private List<MathOp> ops;
    private boolean diverr;
    private BundleColumnBinder sourceBinder;

    public BundleCalculator(String args) {
        String op[] = Strings.splitArray(args, ",");
        ops = new ArrayList<>(op.length);
        for (String o : op) {
            switch (o) {
                case "+":
                case "add":
                    ops.add(new MathOp(Operation.OP_ADD, null));
                    break;
                case "-":
                case "sub":
                    ops.add(new MathOp(Operation.OP_SUB, null));
                    break;
                case "*":
                case "mult":
                case "prod":
                    ops.add(new MathOp(Operation.OP_MULT, null));
                    break;
                case "dmult":
                case "dprod":
                    ops.add(new MathOp(Operation.OP_DMULT, null));
                    break;
                case "/":
                case "div":
                    ops.add(new MathOp(Operation.OP_DIV, null));
                    break;
                case "ddiv":
                    ops.add(new MathOp(Operation.OP_DDIV, null));
                    break;
                case "log":
                    ops.add(new MathOp(Operation.OP_LOG, null));
                    break;
                case "%":
                case "rem":
                    ops.add(new MathOp(Operation.OP_REM, null));
                    break;
                case "=":
                case "s":
                case "set":
                    ops.add(new MathOp(Operation.OP_SET, null));
                    break;
                case "x":
                case "swap":
                    ops.add(new MathOp(Operation.OP_SWAP, null));
                    break;
                case "d":
                case "dup":
                    ops.add(new MathOp(Operation.OP_DUP, null));
                    break;
                case ">":
                case "gt":
                    ops.add(new MathOp(Operation.OP_GT, null));
                    break;
                case ">=":
                case "gteq":
                    ops.add(new MathOp(Operation.OP_GT_EQ, null));
                    break;
                case "<":
                case "lt":
                    ops.add(new MathOp(Operation.OP_LT, null));
                    break;
                case "<=":
                case "lteq":
                    ops.add(new MathOp(Operation.OP_LT_EQ, null));
                    break;
                case "eq":
                    ops.add(new MathOp(Operation.OP_EQ, null));
                    break;
                case "toi":
                case "toint":
                    ops.add(new MathOp(Operation.OP_TOINT, null));
                    break;
                case "tof":
                case "tofloat":
                    ops.add(new MathOp(Operation.OP_TOFLOAT, null));
                    break;
                case "out":
                case "shiftout":
                    ops.add(new MathOp(Operation.OP_SHIFTOUT, null));
                    break;
                case "sqrt":
                    ops.add(new MathOp(Operation.OP_SQRT, null));
                    break;
                case "diverr":
                    diverr = true;
                    break;
                case ">>":
                case "dgt":
                    ops.add(new MathOp(Operation.OP_DGT, null));
                    break;
                case ">>=":
                case "dgteq":
                    ops.add(new MathOp(Operation.OP_DGT_EQ, null));
                    break;
                case "<<":
                case "dlt":
                    ops.add(new MathOp(Operation.OP_DLT, null));
                    break;
                case "<<=":
                case "dlteq":
                    ops.add(new MathOp(Operation.OP_DLT_EQ, null));
                    break;
                case "==":
                case "deq":
                    ops.add(new MathOp(Operation.OP_DEQ, null));
                    break;
                case "min":
                    ops.add(new MathOp(Operation.OP_MIN, null));
                    break;
                case "max":
                    ops.add(new MathOp(Operation.OP_MAX, null));
                    break;
                case "minif":
                    ops.add(new MathOp(Operation.OP_MINIF, null));
                    break;
                case "maxif":
                    ops.add(new MathOp(Operation.OP_MAXIF, null));
                    break;
                case "abs":
                    ops.add(new MathOp(Operation.OP_ABS, null));
                    break;
                case "mean":
                    ops.add(new MathOp(Operation.OP_MEAN, null));
                    break;
                case "variance":
                    ops.add(new MathOp(Operation.OP_VARIANCE, null));
                    break;
                case "vector":
                    ops.add(new MathOp(Operation.OP_VECTOR, null));
                    break;
                default: {
                    if (o.startsWith("c")) {
                        String cols[] = Strings.splitArray(o.substring(1), ":");
                        for (String col : cols) {
                            ops.add(new MathOp(Operation.OP_COLVAL, ValueFactory.create(col)));
                        }
                    } else if (o.startsWith("C")) {
                        String cols[] = Strings.splitArray(o.substring(1), ":");
                        for (String col : cols) {
                            ops.add(new MathOp(Operation.OP_COLNAMEVAL, ValueFactory.create(col)));
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
    }

    /**
     * If the value object contains one or more "," characters then attempt
     * to parse it as an array. Otherwise assume the input is a number.
     */
    private void insertNumbers(LinkedList<ValueNumber> stack, ValueObject input) {
        try {
            String targetString = input.asString().toString();
            if (targetString.indexOf(',') >= 0) {
                String[] targets = targetString.split(",");
                for (int i = 0; i < targets.length; i++) {
                    Number number = NumberFormat.getInstance().parse(targets[i]);
                    if (number instanceof Long) {
                        stack.push(ValueFactory.create(number.longValue()));
                    } else if (number instanceof Double) {
                        stack.push(ValueFactory.create(number.doubleValue()));
                    } else {
                        throw new IllegalStateException(number + " is neither Long nor Double");
                    }
                }
            } else {
                stack.push(input.asNumber());
            }
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Bundle calculate(Bundle line) {
        LinkedList<ValueNumber> stack = new LinkedList<ValueNumber>();
        long maxcol = line.getCount() - 1;
        for (MathOp op : ops) {
            ValueNumber v1, v2;
            switch (op.type) {
                case OP_ADD:
                    v1 = stack.pop();
                    if (v1 == vector) {
                        v1 = stack.pop();
                        while (!stack.isEmpty()) {
                            v1 = v1.sum(stack.pop());
                        }
                        stack.push(v1);
                    } else {
                        v2 = stack.pop();
                        stack.push(v1.sum(v2));
                    }
                    break;
                case OP_SUB:
                    v1 = stack.pop();
                    v2 = stack.pop();
                    stack.push(v2.diff(v1));
                    break;
                case OP_MULT: {
                    v1 = stack.pop();
                    if (v1 == vector) {
                        v1 = stack.pop();
                        long mult = v1.asLong().getLong();
                        while (!stack.isEmpty()) {
                            mult *= stack.pop().asLong().getLong();
                        }
                        stack.push(ValueFactory.create(mult));
                    } else {
                        v2 = stack.pop();
                        long mult = v1.asLong().getLong() * v2.asLong().getLong();
                        stack.push(ValueFactory.create(mult));
                    }
                    break;
                }
                case OP_DMULT:  {
                    v1 = stack.pop();
                    if (v1 == vector) {
                        v1 = stack.pop();
                        double mult = v1.asDouble().getDouble();
                        while (!stack.isEmpty()) {
                            mult *= stack.pop().asDouble().getDouble();
                        }
                        stack.push(ValueFactory.create(mult));
                    } else {
                        v2 = stack.pop();
                        double mult = v1.asDouble().getDouble() * v2.asDouble().getDouble();
                        stack.push(ValueFactory.create(mult));
                    }
                    break;
                }
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
                case OP_COLVAL: {
                    ValueObject target = getSourceColumnBinder(line).getColumn(line, (int) op.val.asLong().getLong());
                    insertNumbers(stack, target);
                    break;
                }
                case OP_COLNAMEVAL: {
                    ValueObject target = line.getValue(line.getFormat().getField(op.val.toString()));
                    insertNumbers(stack, target);
                    break;
                }
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
                    if (v2.asDouble().getDouble() != v1.asDouble().getDouble()) {
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
                    if (v2.asLong().getLong() != v1.asLong().getLong()) {
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
                    int col = (int) stack.pop().asLong().getLong();
                    ValueNumber val = stack.pop();
                    if (col < 0 || col > maxcol) {
                        getSourceColumnBinder(line).appendColumn(line, val);
                    } else {
                        getSourceColumnBinder(line).setColumn(line, col, val);
                    }
                    break;
                case OP_MIN:
                    v1 = stack.pop();
                    if (v1 == vector) {
                        v1 = stack.pop();
                        while (!stack.isEmpty()) {
                            v1 = v1.min(stack.pop());
                        }
                        stack.push(v1);
                    } else {
                        v2 = stack.pop();
                        stack.push(v1.min(v2));
                    }
                    break;
                case OP_MAX:
                    v1 = stack.pop();
                    if (v1 == vector) {
                        v1 = stack.pop();
                        while (!stack.isEmpty()) {
                            v1 = v1.max(stack.pop());
                        }
                        stack.push(v1);
                    } else {
                        v2 = stack.pop();
                        stack.push(v1.max(v2));
                    }
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
                case OP_VECTOR:
                    stack.push(vector);
                    break;
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

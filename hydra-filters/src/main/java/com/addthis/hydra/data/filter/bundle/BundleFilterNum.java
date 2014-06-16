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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.util.BundleCalculator;


/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">performs postfix calculator operations</span>.
 * <p/>
 * <p>This filter uses a calculation stack to perform arithmetic operations. The stack is initially empty
 * and values can be pushed or popped from the top of the stack. Arithmetic operations can be performed
 * on the element(s) at the top of the stack.
 * <p>The values from the input bundle can be pushed onto the stack
 * and the value at the top of the stack can be moved into the input bundle.
 * Direct arithmetic operations on the input bundle is not permitted.
 * If the 'columns' parameter is missing then the entire input bundle is available
 * to the calculation stack. Otherwise a subset of the input bundle is available
 * to the calculation stack. It is convenient to specify the 'columns' parameter
 * as the user must know the order of the elements in the input bundle. The 'columns'
 * parameter specifies the order of the subset of elements.</p>
 * <p>The conditional operations will terminate the calculation stack when the condition
 * is not met. If the calculation is terminated early then the filter returns false. Otherwise
 * the filter returns true.</p>
 * <p>The following operations below are available. Each operation proceeds in three steps:
 * (1) Zero or more values are popped off the stack, (2) an operation is performed on these values,
 * and (3) zero or more values are pushed onto the stack. For convenience the following table
 * labels the first element popped off the stack as "a" (the topmost element), the second
 * element as "b", etc. The variables "X","Y","Z", etc. represent numbers in the command sequence.
 * <table width="80%" class="num">
 * <tr>
 * <th width="15%">name</th>
 * <th width="10%">pop count</th>
 * <th width="65%">operation</th>
 * <th width="10%">push count</th>
 * </tr>
 * <tr>
 * <td>"+" or "add"</td>
 * <td>2</td>
 * <td>add a and b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"-" or "sub"</td>
 * <td>2</td>
 * <td>subtract a from b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"*" or "mult" or "prod"</td>
 * <td>2</td>
 * <td>long integer multiplication</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"dmult" or "dprod"</td>
 * <td>2</td>
 * <td>floating point multiplication</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"/" or "div"</td>
 * <td>2</td>
 * <td>long integer divide the b by a.
 * <br>If diverr is false and a is 0 then return 0.</br></td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"ddiv"</td>
 * <td>2</td>
 * <td>floating point divide the b by a.
 * <br>If diverr is false and a is 0.0 then return 0.</br></td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"log"</td>
 * <td>1</td>
 * <td>compute logarithm base 10 of a</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"%" or "rem"</td>
 * <td>2</td>
 * <td>modulo b by a
 * <br>If diverr is false and a is 0 then return 0.</br></td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"=" or "s" or "set"</td>
 * <td>2</td>
 * <td>assign the a-th column the value of b.
 * <br>If a is out of bounds then
 * append b to the end of the bundle</br></td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"x" or "swap"</td>
 * <td>2</td>
 * <td>swap the positions of a and b</td>
 * <td>2</td>
 * </tr>
 * <tr>
 * <td>"d" or "dup"</td>
 * <td>1</td>
 * <td>duplicate element a</td>
 * <td>2</td>
 * </tr>
 * <tr>
 * <td>">" or "gt"</td>
 * <td>2</td>
 * <td>if long integer (b > a) is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>">=" or "gteq"</td>
 * <td>2</td>
 * <td>if long integer (b >= a) is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"<" or "lt"</td>
 * <td>2</td>
 * <td>if long integer (b < a) is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"<=" or "lteq"</td>
 * <td>2</td>
 * <td>if long integer (b <= a) is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"eq"</td>
 * <td>2</td>
 * <td>if long integer (a == b) is false then end the calculation</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"toi" or "toint"</td>
 * <td>1</td>
 * <td>convert a to a long integer</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"tof" or "tofloat"</td>
 * <td>1</td>
 * <td>convert a to a floating-point value</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"tob" or "tobits"</td>
 * <td>1</td>
 * <td>convert floating point a to its bitwise long representation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"btof" or "btofloat"</td>
 * <td>1</td>
 * <td>convert the bitwise representation of long a to a floating-point value</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"out" or "shiftout"</td>
 * <td>1</td>
 * <td>append a to the end of the bundle</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>"sqrt"</td>
 * <td>1</td>
 * <td>calculate the square root of a</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"diverr"</td>
 * <td>0</td>
 * <td>set the diverr flag to true</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>">>" or "dgt"</td>
 * <td>2</td>
 * <td>if floating-point (b > a) is false then end the calculation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>">>=" or "dgteq"</td>
 * <td>2</td>
 * <td>if floating-point (b >= a) is false then end the calculation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"<<" or "dlt"</td>
 * <td>2</td>
 * <td>if floating-point (b < a) is false then end the calculation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"<<=" or "dlteq"</td>
 * <td>2</td>
 * <td>if floating-point (b <= a) is false then end the calculation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"==" or "deq"</td>
 * <td>2</td>
 * <td>if floating-point (b == a) is false then end the calculation</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"min"</td>
 * <td>2</td>
 * <td>return a copy of the minimum of a and b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"max"</td>
 * <td>2</td>
 * <td>return a copy of the maximum of a and b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"minif"</td>
 * <td>2</td>
 * <td>return the minimum of a and b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"maxif"</td>
 * <td>2</td>
 * <td>return the maximum of a and b</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"abs"</td>
 * <td>1</td>
 * <td>return the absolute value of a as a float</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"mean"</td>
 * <td>ALL</td>
 * <td>consumes all values on the stack and returns the mean</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"variance"</td>
 * <td>ALL</td>
 * <td>consumes all values on the stack and returns the (population) variance</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"cX:Y:Z..."</td>
 * <td>0</td>
 * <td>push the value in columns X, Y, Z,... onto the stack</td>
 * <td>len(args)</td>
 * </tr>
 * <tr>
 * <td>"nX:Y:Z..."</td>
 * <td>0</td>
 * <td>push the values X, Y, Z,... onto the stack</td>
 * <td>len(args)</td>
 * </tr>
 * <tr>
 * <td>"vX"</td>
 * <td>0</td>
 * <td>push the value X onto the stack</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>"vector"</td>
 * <td>0</td>
 * <td>modifies the behavior of the next operation.<br>
 * Next operation pops all elements off the stack<br>
 * and pushes a single value onto the stack. Available<br>
 * for "+", "mult", "dmult", "min", and "max".
 * <td>0</td>
 * </tr>
 * </table>
 * <p>Examples:</p>
 * <pre>
 *   {op: "num", columns : ["END_TIME", "START_TIME", "WALL_TIME"], define : "c0,c1,sub,v1000,ddiv,toint,v2,set"},
 *   {op: "num", columns : ["WALL_TIME", "TASKS", "CLUSTER_TIME"], define : "c0,c1,mult,v2,set"},
 * </pre>
 *
 * @user-reference
 * @hydra-name num
 */
public class BundleFilterNum extends BundleFilter {

    /**
     * Sequence of commands to execute (comma-delimited)
     */
    @Codec.Set(codable = true, required = true)
    private String define;

    /**
     * Subset of fields from the bundle filter that are used in calculation.
     */
    @Codec.Set(codable = true)
    private String[] columns;

    private BundleCalculator calculator;


    public BundleFilterNum setDefine(String define) {
        this.define = define;
        return this;
    }

    @Override
    public void initialize() {
        calculator = new BundleCalculator(define);

    }

    protected Bundle makeAltBundle(Bundle bundle) {
        ListBundleFormat format = new ListBundleFormat();
        Bundle alt = new ListBundle(format);
        for (int i = 0; i < columns.length; i++) {
            BundleField field = format.getField(columns[i]);
            alt.setValue(field, bundle.getValue(bundle.getFormat().getField(columns[i])));
        }
        return alt;
    }

    protected void mergeBundles(Bundle orig, Bundle nBundle) {
        for (BundleField bf : nBundle) {
            orig.setValue(orig.getFormat().getField(bf.getName()), nBundle.getValue(bf));
        }
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        if (columns == null) {
            return calculator.calculate(bundle) != null;
        } else {
            Bundle alt = makeAltBundle(bundle);
            boolean res = calculator.calculate(alt) != null;
            mergeBundles(bundle, alt);
            return res;
        }

    }
}

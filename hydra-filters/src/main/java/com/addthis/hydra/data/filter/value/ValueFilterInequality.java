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
package com.addthis.hydra.data.filter.value;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns those values that satisfies the specified inequalities</span>.
 * <p/>
 * <p>The input value is placed on the left-hand side of the inequality.
 * The {@link #iop iop} field must be one of "lt" (&lt;), "lteq" (&lt;=), "eq" (==), "gteq" (&gt;=), or "gt" (&gt;).
 * Either the rh field or rhd field must be specified but not both fields.</p>
 * <p/>
 * <p>When the inequality fails to hold then null is returned. If the input is null then the output is null.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"TL_C", filter:{op:"inequality", iop:"lteq", rh:10000}}
 * </pre>
 *
 * @user-reference
 */

public class ValueFilterInequality extends AbstractValueFilter {

    /**
     * The inequality to perform.
     */
    @FieldConfig(codable = true, required = true)
    private String iop;

    /**
     * The right-hand side as a long value.
     */
    @FieldConfig(codable = true)
    private Long rh;

    /**
     * The right-hand side as a double value.
     */
    @FieldConfig(codable = true)
    private Double rhd;

    // todo: int versus float, etc

    public ValueFilterInequality() {
    }

    public ValueFilterInequality(String iop, Long rh) {
        this.iop = iop;
        this.rh = rh;
    }

    // pass through if true, else null
    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return value;
        }
        try {
            Boolean res = false;
            if (rh != null) {
                long val = ValueUtil.asNumberOrParseLong(value, 10).asLong().getLong();
                res = evalExpression(iop, val, rh);
            } else if (rhd != null) {
                double val = ValueUtil.asNumberOrParseDouble(value).asDouble().getDouble();
                res = evalExpressionD(iop, val, rhd);
            }
            if (res) {
                return value;
            } else {
                return null;
            }
        } catch (Exception ex) {
            return value;
        }
    }

    protected boolean evalExpression(String op, long val, long rh) {
        if (op.equals("gteq")) {
            return val >= rh;
        } else if (op.equals("lteq")) {
            return val <= rh;
        } else if (op.equals("lt")) {
            return val < rh;

        } else if (op.equals("gt")) {
            return val > rh;
        } else if (op.equals("eq")) {
            return val == rh;
        }

        return false;
    }

    protected boolean evalExpressionD(String op, double val, double rh) {
        if (op.equals("gteq")) {
            return val >= rh;
        } else if (op.equals("lteq")) {
            return val <= rh;
        } else if (op.equals("lt")) {
            return val < rh;

        } else if (op.equals("gt")) {
            return val > rh;
        } else if (op.equals("eq")) {
            return val == rh;
        }

        return false;
    }
}

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
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">filters rows based on arithmetic comparison</span>.
 * <p/>
 * <p>The notation is compare=[column number]:[operation]:[value]. Operation can be "lt", "lteq",
 * "eq", "gteq", or "gt". Only rows that satisfy the comparison are kept in the results.</p>
 * <p/>
 * <pre>
 * compare=col:op:value
 *
 *
 * Example:
 *
 * Alpha 100
 * Beta 20
 * Gamma 90
 * Delta 40
 *
 * compare=0:gteq:90
 *
 * Alpha 100
 * Gamma 90
 * </pre>
 *
 * @user-reference
 * @hydra-name compare
 */
public class OpCompare extends AbstractRowOp {

    private int col;
    private String op;
    private long compValue;

    public OpCompare(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        try {
            String[] opt = args.split(":");
            if (opt.length == 3) {
                col = Integer.parseInt(opt[0]);
                op = opt[1];
                compValue = Long.parseLong(opt[2]);

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (col < row.getCount()) {
            long valLong = 0;
            BundleColumnBinder binder = getSourceColumnBinder(row);
            ValueObject val = binder.getColumn(row, col);
            try {
                valLong = Long.parseLong(val.toString());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            if (performOp(valLong, compValue, op)) {
                binder.setColumn(row, col, val);
                return row;
            } else {
                return null;
            }
        }
        return row;
    }

    public boolean performOp(Long val1, Long val2, String op) {
        if (op.endsWith("eq")) {
            if (val1.equals(val2)) {
                return true;
            }
        }
        if (op.startsWith("g")) {
            return (val1 > val2);
        }
        if (op.startsWith("l")) {
            return (val1 < val2);
        }
        return false;
    }
}

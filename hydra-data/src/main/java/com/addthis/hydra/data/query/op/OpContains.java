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


/**
 * <p>This query operation <span class="hydra-summary">filters rows based on substring matching</span>.
 * <p/>
 * <p>The notation is contains=[column number]:[string1],[string2],[etc].
 * If the column value contains as a substring one or more of the specified
 * strings then the row is included in the results. Otherwise the rows is excluded.</p>
 * <p/>
 * <pre>
 * contains=col:string1,string2,etc
 *
 *
 * Example:
 *
 * Alpha 4
 * 3Alpha2 5
 * Beta 3
 * Sigma 5
 *
 * contains=0:Alpha
 *
 * Alpha 4
 * 3Alpha2 5
 * </pre>
 *
 * @user-reference
 * @hydra-name contains
 */
public class OpContains extends AbstractRowOp {

    private int col;
    private String[] contains;

    public OpContains(String args) {
        try {
            String opt[] = args.split(":");
            if (opt.length == 2) {
                col = Integer.parseInt(opt[0]);
                contains = opt[1].split(",");

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (col < row.getCount()) {
            BundleColumnBinder binder = getSourceColumnBinder(row);
            ValueObject val = binder.getColumn(row, col);
            for (String elt : contains) {
                if (elt.length() > 0 && val.toString().indexOf(elt) != -1) {
                    binder.setColumn(row, col, val);
                    return row;
                }
            }
            return null;
        }
        return row;
    }
}

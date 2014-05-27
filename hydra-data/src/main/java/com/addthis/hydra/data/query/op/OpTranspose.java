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
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">transposes rows and columns</span>.
 *
 * @user-reference
 * @hydra-name trans or t
 */
public class OpTranspose extends AbstractTableOp {

    public OpTranspose(DataTableFactory tableFactory, ChannelProgressivePromise queryPromise) {
        super(tableFactory, queryPromise);
    }

    @Override
    public DataTable tableOp(DataTable tableIn) {
        if (tableIn.size() == 0) {
            return tableIn;
        }
        int rowsIn = tableIn.size();
        int colsIn = tableIn.get(0).getFormat().getFieldCount();
        DataTable tableOut = createTable(colsIn);
        BundleField[] fieldIn = new BundleColumnBinder(tableIn.get(0)).getFields();
        BundleField[] fieldOut = new BundleField[rowsIn];
        for (int i = 0; i < rowsIn; i++) {
            fieldOut[i] = tableOut.getFormat().getField(Integer.toString(i));
        }
        for (int i = 0; i < colsIn; i++) {
            int colOut = 0;
            Bundle rowOut = tableOut.createBundle();
            for (Bundle rowIn : tableIn) {
                ValueObject fromValue = rowIn.getValue(fieldIn[i]);
                //this check turns explicit nulls into skips but it is the most correct we can get until we fix
                //  the whole bundle / field / binding / index / query op / table mess
                if (fromValue != null) {
                    rowOut.setValue(fieldOut[colOut], fromValue);
                }
                colOut++;
            }
            tableOut.append(rowOut);
        }
        return tableOut;
    }

}

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
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryOpProcessor;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">reverses the ordering of rows</span>.
 * <p/>
 * <p>In a table with rows numbered 1,2,..,N-1,N, this operation moves row 1 to row N,
 * row 2 to row N-1, ..., row N-1 to row 2, and row N to row 1.</p>
 *
 * @user-reference
 * @hydra-name reverse
 */
public class OpReverse extends AbstractTableOp {

    public OpReverse(QueryOpProcessor processor, ChannelProgressivePromise queryPromise) {
        super(processor, queryPromise);
    }

    @Override
    public DataTable tableOp(DataTable result) {
        int size = result.size();
        for (int i = 0; i < size / 2; i++) {
            Bundle a = result.get(i);
            Bundle b = result.get(size - i - 1);
            result.set(size - i - 1, a);
            result.set(i, b);
        }
        return result;
    }

}

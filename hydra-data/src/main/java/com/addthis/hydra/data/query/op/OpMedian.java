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

import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">returns the middle row</span>.
 * <p/>
 * <p>All the rows in the output are deleted and replaced with the middle row.</p>
 * <p/>
 * <p>Examples:</p>
 * <pre>
 *     median
 * </pre>
 *
 * @user-reference
 * @hydra-name median
 */
public class OpMedian extends AbstractTableOp {

    public OpMedian(DataTableFactory processor, ChannelProgressivePromise queryPromise) {
        super(processor, queryPromise);
    }

    @Override
    public DataTable tableOp(DataTable result) {
        DataTable ret = createTable(1);
        ret.append(result.get(result.size() / 2));
        return ret;
    }
}

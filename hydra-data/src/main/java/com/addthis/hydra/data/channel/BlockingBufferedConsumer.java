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
package com.addthis.hydra.data.channel;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;


/**
 * table accumulator for op chain that can block until complete send or error
 */
public class BlockingBufferedConsumer extends BlockingNullConsumer {

    private final DataTable table;

    public BlockingBufferedConsumer() throws InterruptedException {
        table = Query.createProcessor(this).createTable(0);
    }

    public BlockingBufferedConsumer(String ops[]) throws InterruptedException {
        table = Query.createProcessor(this, ops).createTable(0);
    }

    public BlockingBufferedConsumer(QueryOpProcessor proc) throws InterruptedException {
        table = proc.createTable(0);
    }

    @Override
    public void send(Bundle row) {
        table.append(row);
    }

    /**
     * only callable once
     */
    public DataTable getTable() throws Exception {
        waitComplete();
        return table;
    }
}

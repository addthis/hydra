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
package com.addthis.hydra.data.query;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * this class of operator should act as it's own source.
 * it will create a new bundle stream by fully consuming
 * and discarding the previous input.
 */
public abstract class AbstractTableOp extends AbstractQueryOp implements QueryTableOp, DataTableFactory {

    protected DataTable table;

    private final DataTableFactory tableFactory;
    private final ChannelProgressivePromise queryPromise;

    public AbstractTableOp(DataTableFactory tableFactory, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.tableFactory = tableFactory;
        this.queryPromise = queryPromise;
    }

    public abstract DataTable tableOp(DataTable table);

    public DataTable createTable(int size) {
        return tableFactory.createTable(size);
    }

    public DataTable getTable() {
        if (table == null) {
            table = tableFactory.createTable(0);
        }
        return table;
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        getTable().append(row);
    }

    @Override
    public void sendComplete() {
        table = tableOp(getTable());
        complete();
    }

    @Override
    public void sendTable(DataTable table) {
        this.table = tableOp(table);
        complete();
    }

    private void complete() {
        QueryOp next = getNext();
        if (next != null) {
            next.sendTable(getTable());
        }
    }
}


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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.table.DataTableListWrapper;


public class ResultTable extends DataTableListWrapper implements DataTableFactory {

    private final DataTableFactory factory;

    public ResultTable(DataTableFactory factory, List<Bundle> list) {
        super(list);
        this.factory = factory;
    }

    public ResultTable(DataTableFactory factory, int size) {
        super(size > 0 ? new ArrayList<Bundle>(size) : new LinkedList<Bundle>());
        this.factory = factory;
    }

    public ResultTable(DataTableFactory factory, Bundle row) {
        this(factory, 1);
        append(row);
    }

    @Override
    public DataTable createTable(int sizeHint) {
        return factory.createTable(sizeHint);
    }
}

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


import com.addthis.bundle.table.DataTable;

/**
 * normal lifecycle:
 * <p/>
 * setTable() -- once
 * send(Bundle) -- one or more times
 * close() -- once
 * getTableOutput() -- once
 */
public interface QueryTableOp extends QueryOp {

    /**
     * when a tableop is followed by a table op, this
     * allows an optimized transfer of internal table state.
     */
    @Override public void sendTable(DataTable table);
}

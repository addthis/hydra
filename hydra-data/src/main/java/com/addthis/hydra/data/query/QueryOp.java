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

import java.io.Closeable;

import com.addthis.bundle.core.BundleOutput;
import com.addthis.bundle.table.DataTable;

/**
 * normal lifecycle:
 *
 *  send() ...
 *  sendComplete() -- once
 *
 *  OR
 *
 *  sendTable()
 *
 *  THEN
 *
 *  close() -- once
 */

/**
 * This section of the job specification handles query operations.
 * <p/>
 * <p>Query operations on Hydra tree output.</p>
 *
 * @user-reference
 * @hydra-category
 */
public interface QueryOp extends BundleOutput, Closeable {

    /**
     * @param next next op in the op chain
     */
    public void setNext(QueryMemTracker memTracker, QueryOp next);

    /**
     * return next op in the chain
     */
    public QueryOp getNext();

    /**
     * return query memory tracker
     */
    public QueryMemTracker getMemTracker();

    /**
     * @return true if this is an instance of table op
     */
    public void sendTable(DataTable table, QueryStatusObserver queryStatusObserver);

    /**
     * @return simple debugging name
     */
    public String getSimpleName();
}

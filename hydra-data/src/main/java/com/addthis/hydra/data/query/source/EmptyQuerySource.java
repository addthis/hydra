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
package com.addthis.hydra.data.query.source;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;

public class EmptyQuerySource implements QuerySource {

    @Override
    public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException {
        QueryHandle handle = new QueryHandle() {
            @Override
            public void cancel(String message) {
            }
        };
        consumer.sendComplete();
        return handle;
    }

    @Override
    public void noop() {
        // do nothing
    }

    @Override
    public boolean isClosed() {
        // empty source can't be canceled, it completes immediately when it is created
        return false;
    }
}

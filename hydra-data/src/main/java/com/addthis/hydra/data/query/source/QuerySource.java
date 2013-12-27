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


public interface QuerySource {

    /**
     * issues an async query with results coming in to a consumer
     */
    public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException;

    /**
     * used to signal the source was not used to query and can be returned to a
     * resource pool happens when a source comes from a pool and is subsequently
     * handed to a cache, but the cache returns a response not requiring use of
     * the source.
     */
    public void noop();

    /**
     * @return true if this source has been closed
     */
    public boolean isClosed();
}

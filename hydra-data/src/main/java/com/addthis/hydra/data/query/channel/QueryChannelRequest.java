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
package com.addthis.hydra.data.query.channel;

import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.Query;


/**
 * for channel command
 */
public class QueryChannelRequest implements Codec.Codable {

    @Codec.Set(codable = true)
    private Query query;
    @Codec.Set(codable = true)
    private int queryId;
    @Codec.Set(codable = true)
    private boolean cancel;
    @Codec.Set(codable = true)
    private boolean ping;

    // default constructor
    public QueryChannelRequest() {
    }

    public QueryChannelRequest setQuery(Query query) {
        this.query = query;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public QueryChannelRequest cancel() {
        cancel = true;
        return this;
    }

    public boolean isCanceled() {
        return cancel;
    }

    public int getQueryID() {
        return queryId;
    }

    public QueryChannelRequest setQueryID(int id) {
        queryId = id;
        return this;
    }

    public QueryChannelRequest setPing(boolean ping) {
        this.ping = ping;
        return this;
    }

    public boolean isPing() {
        return ping;
    }
}

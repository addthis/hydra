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


public class QueryHost implements Codec.Codable {

    @Codec.Set(codable = true, required = true)
    private String host;
    @Codec.Set(codable = true, required = true)
    private int port;

    public QueryHost(String host, int port) {
        setHost(host);
        setPort(port);
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public String toString() {
        return "QH:" + host + ":" + port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public QueryHost setHost(String host) {
        if (host == null) {
            throw new NullPointerException("host cannot be null");
        }
        this.host = host;
        return this;
    }

    public QueryHost setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryHost) {
            QueryHost q = (QueryHost) o;
            return q.host.equals(host) && q.port == port;
        }
        return false;
    }
}

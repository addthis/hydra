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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * multi-client that connects to multiple query channels and provides optional
 * intermediate operators on the results before returning them.
 */
public abstract class QueryChannelClientX implements QuerySource {

    private static final Logger log = LoggerFactory.getLogger(QueryChannelClientX.class);

    private HashSet<QueryHost> hosts = new HashSet<QueryHost>();

    public QueryChannelClientX() {
    }

    public QueryChannelClientX(QueryHost host) {
        addHost(host);
    }

    public QueryChannelClientX(Collection<QueryHost> hosts) {
        addHosts(hosts);
    }

    public QueryChannelClientX addHosts(Collection<QueryHost> hosts) {
        this.hosts.addAll(hosts);
        return this;
    }

    public QueryChannelClientX addHost(QueryHost host) {
        hosts.add(host);
        return this;
    }

    public QueryChannelClientX removeHost(QueryHost host) {
        hosts.remove(host);
        return this;
    }

    /** */
    public abstract QuerySource leaseClient(QueryHost host);

    /** */
    public abstract void releaseClient(QuerySource channel);

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public synchronized QueryHandle query(final Query query, final DataChannelOutput consumer) throws QueryException {
        QueryHandle handle;
        try {
            handle = new QueryHandle() {
                final ResultProxy proxy = new ResultProxy(query, consumer);

                @Override
                public void cancel(String message) {
                    proxy.cancelQuery(message);
                }
            };
        } catch (Exception e) {
            throw new QueryException(e);
        }
        return handle;
    }

    /**
     * proxy to watch for end of query to enable state cleanup
     */
    private class ResultProxy implements DataChannelOutput {

        final List<QueryHandle> handles;
        final List<QuerySource> clients;
        final DataChannelOutput consumer;
        final AtomicInteger clientCount;
        final Query query;

        ResultProxy(Query query, DataChannelOutput consumer) throws Exception {
            this.query = query;
            this.consumer = consumer;

            int proxied = hosts.size();
            if (proxied == 0) {
                throw new QueryException("no hosts specified");
            }
            this.clientCount = new AtomicInteger(proxied);
            this.handles = new ArrayList<QueryHandle>(proxied);
            this.clients = new ArrayList<QuerySource>(proxied);
            if (query.isTraced()) {
                Query.emitTrace(query.uuid() + " proxy " + consumer + " to " + hosts);
            }
            for (QueryHost host : hosts) {
                try {
                    QuerySource client = leaseClient(host);
                    clients.add(client);
                    if (query.isTraced()) {
                        Query.emitTrace(query.uuid() + " adding " + host + " " + client);
                    }
                    handles.add(client.query(query, this));
                } catch (DataChannelError ex) {
                    consumer.sourceError(ex);
                    break;
                } catch (Exception ex) {
                    consumer.sourceError(DataChannelError.promote(ex));
                    break;
                }
            }
        }

        @Override
        public void send(Bundle row) throws DataChannelError {
            try {
                consumer.send(row);
            } catch (DataChannelError ex) {
                error(ex);
                throw ex;
            } catch (Exception ex) {
                error(ex);
                throw new DataChannelError(ex);
            }
        }

        @Override
        public void send(List<Bundle> bundles) {
            if (bundles != null && !bundles.isEmpty()) {
                for (Bundle bundle : bundles) {
                    send(bundle);
                }
            }
        }

        @Override
        public void sendComplete() {
            if (clientCount.decrementAndGet() == 0) {
                /**
                 * only deliver complete message one time once last client
                 * completes
                 */
                queryComplete();
            }
            if (query.isTraced()) {
                Query.emitTrace(query.uuid() + " result complete remain=" + clientCount);
            }
        }

        private void error(Exception ex) {
            if (clientCount.decrementAndGet() == 0) {
                queryComplete();
            }
            if (query.isTraced()) {
                Query.emitTrace(query.uuid() + " result error remain=" + clientCount);
            }
        }

        private void queryComplete() {
            consumer.sendComplete();
            for (QuerySource client : clients) {
                releaseClient(client);
            }
        }

        private synchronized void cancelQuery(String message) {
            for (QueryHandle handle : handles) {
                handle.cancel(message);
            }
        }

        @Override
        public Bundle createBundle() {
            return consumer.createBundle();
        }

        @Override
        public void sourceError(DataChannelError er) {
            consumer.sourceError(er);
        }
    }
}

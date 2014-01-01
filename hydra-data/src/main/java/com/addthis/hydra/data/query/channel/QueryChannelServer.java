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

import java.io.EOFException;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * should be abstracted further to be a generic pipe for queries and results.
 * services queries over a QueryChannel.
 *
 * TODO implement interrupt/cancel
 */
public class QueryChannelServer extends Thread {

    private static final Logger log = LoggerFactory.getLogger(QueryChannelServer.class);

    private static final int maxQueryTime = Parameter.intValue("qmaster.maxQueryTime", 24 * 60 * 60); // one day upper bound on how long we'll let a query run
    private static final Timer queryTimes = Metrics.newTimer(QueryChannelServer.class, "queryTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Meter errorRate = Metrics.newMeter(getClass(), "errorRate", "errorRate", TimeUnit.SECONDS);

    private final ServerSocket server;
    private final QuerySource source;
    private final HashSet<ConnectionHandler> open = new HashSet<ConnectionHandler>();

    public QueryChannelServer(int port, QuerySource source) throws IOException {
        this.server = new ServerSocket(port);
        this.source = source;
        setDaemon(true);
        setName("QueryChannelServer port=" + server.getLocalPort() + " src=" + source);
    }

    public int getPort() {
        return server.getLocalPort();
    }

    public void close() {
        try {
            server.close();
        } catch (IOException e)  {
            log.warn("", e);
        }
        interrupt();
        synchronized (open) {
            for (ConnectionHandler handler : open) {
                log.warn("interrupting open connection : " + handler);
                handler.interrupt();
            }
        }
    }

    public void run() {
        while (true) {
            try {
                new ConnectionHandler(server.accept()).start();
            } catch (SocketException e) {
                // normal during shutdown
                break;
            } catch (IOException e)  {
                log.warn("", e);
                break;
            }
        }
    }

    /**
     * one thread per socket connection -- serialized queries
     */
    private class ConnectionHandler extends Thread {

        private QueryChannel<QueryChannelResponse, QueryChannelRequest> channel;
        private final HashMap<Integer, QueryHandler> channelMap = new HashMap<Integer, QueryHandler>();
        private long pingCount = 0;

        ConnectionHandler(Socket socket) throws IOException {
            super("QCSocket to " + socket.getRemoteSocketAddress());
            setDaemon(true);
            this.channel = new QueryChannel<QueryChannelResponse, QueryChannelRequest>(socket, QueryChannelRequest.class);
            if (log.isDebugEnabled()) {
                log.debug("new connection " + socket);
            }
        }

        public void run() {
            synchronized (open) {
                open.add(this);
            }
            try {
                while (handleRequest()) {
                    // loop forever until false
                }
                if (log.isDebugEnabled()) log.debug("Connection handler shutting down");
                synchronized (channelMap) {
                    for (QueryHandler handler : channelMap.values()) {
                        log.warn("interrupting @ shutdown " + handler);
                        handler.cancel("shutting down");
                    }
                }
            } finally {
                synchronized (open) {
                    open.remove(this);
                }
            }
        }

        private void startHandler(QueryHandler handler) {
            synchronized (channelMap) {
                channelMap.put(handler.queryID, handler);
            }
            handler.start();
        }

        private QueryHandler getHandler(Integer id) {
            synchronized (channelMap) {
                return channelMap.get(id);
            }
        }

        private QueryHandler removeHandler(Integer id) {
            synchronized (channelMap) {
                return channelMap.remove(id);
            }
        }

        private boolean handleRequest() {
            try {
                QueryChannelRequest request = channel.recvSync();
                if (request.isCanceled()) {
                    QueryHandler handler = getHandler(request.getQueryID());
                    if (handler != null) {
                        log.warn("interrupting " + handler.query.uuid() + " b/c canceled " + request);
                        handler.cancel("canceled");
                    }
                } else if (request.isPing()) {
                    if (++pingCount % 30 == 0 && log.isDebugEnabled()) {
                        log.warn("Received ping #: " + pingCount);
                    }
                    channel.sendSync(new QueryChannelResponse().setPing(true));
                } else if (request.getQuery() != null) {
                    startHandler(new QueryHandler(request.getQueryID(), request.getQuery()));
                }
                return true;
            } catch (EOFException e) {
                if (log.isWarnEnabled()) {
                    log.warn("[handler] EOF " + e, e);
                }
            } catch (SocketException e) {
                if (log.isWarnEnabled()) {
                    // normal during query cancel
                    log.warn("[handler] Socket Exception " + e, e);
                }
            } catch (Exception e) {
                log.warn("[handler] error " + e, e);
            } finally {
                channel.flush();
            }
            return false;
        }

        /** */
        private class QueryHandler extends Thread implements DataChannelOutput {

            private int rows;
            private long start;
            private QueryHandle handle;
            private final Query query;
            private final Integer queryID;
            private final int queryTimeout;
            private final Semaphore lock = new Semaphore(1);
            private final ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
            private final FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();
            private final ListBundleFormat format = new ListBundleFormat();

            private QueryHandler(Integer queryID, Query query) {
                super("QCHandler " + queryID + " " + query.uuid());
                this.queryID = queryID;
                this.query = query;
                this.queryTimeout = Integer.parseInt(query.getParameter("timeout", Integer.toString(maxQueryTime)));
            }

            public void cancel(String message) {
                if (handle != null) {
                    handle.cancel(message);
                }
            }

            public void run() {
                try {
                    start = System.currentTimeMillis();
                    if (query.isTraced()) {
                        Query.emitTrace("[handler] start " + query.uuid() + " --> " + query);
                    }
                    acquireLock("Timed out waiting to acquire query start lock after: " + maxQueryTime + " seconds, please retry your query");
                    handle = source.query(query, this);
                    acquireLock("Timed out waiting for query handle after submitting query, waited: " + maxQueryTime + " seconds, please retry your query");
                    if (query.isTraced()) {
                        Query.emitTrace("[handler] finish " + query.uuid() + " rows=" + rows + " in " + (System.currentTimeMillis() - start) + " ms");
                    }
                } catch (Exception e) {
                    try {
                        log.warn("[handler] error " + query.uuid(), e);
                        channel.sendSync(new QueryChannelResponse().setError(e.getMessage()).setQueryID(queryID));
                        errorRate.mark();
                    } catch (SocketException e1) {
                        log.warn("[handler] socket error " + e1);
                    } catch (Exception e1)  {
                        log.warn("", e1);
                    }
                } finally {
                    channel.flush();
                    removeHandler(queryID);
                }
            }

            private void acquireLock(String message) throws InterruptedException {
                if (queryTimeout <= 0) {
                    lock.acquire();
                } else if (!lock.tryAcquire(queryTimeout, TimeUnit.SECONDS)) {
                    log.warn(message);
                    throw new QueryException(message);
                }
            }

            @Override
            public void send(Bundle row) {
                try {
                    channel.sendSync(new QueryChannelResponse().addRow(DataChannelCodec.encodeBundle(row, fieldMap, classMap)).setQueryID(queryID));
                    rows++;
                } catch (Exception e) {
                    cancel(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void send(List<Bundle> bundles) {
                try {
                    QueryChannelResponse queryChannelResponse = new QueryChannelResponse().setQueryID(queryID);
                    for (Bundle bundle : bundles) {
                        queryChannelResponse.addRow(DataChannelCodec.encodeBundle(bundle, fieldMap, classMap));
                    }
                    channel.sendSync(queryChannelResponse);
                    rows++;
                } catch (Exception e) {
                    cancel(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void sendComplete() {
                try {
                    channel.sendSync(new QueryChannelResponse().setEnd(true).setQueryID(queryID));
                    queryTimes.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    cancel(e.getMessage());
                    throw new RuntimeException(e);
                } finally {
                    lock.release();
                }
            }

            @Override
            public void sourceError(DataChannelError ex) {
                try {
                    channel.sendSync(new QueryChannelResponse().setError(ex.getMessage()).setQueryID(queryID));
                    queryTimes.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    errorRate.mark();
                } catch (SocketException e) {
                    // sometimes the error is do to the socket being broken, we can ignore this error
                    log.warn("Unable to send error message because the socket was closed, error was: " + ex.getMessage());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    lock.release();
                }
            }

            @Override
            public Bundle createBundle() {
                return new ListBundle(format);
            }
        }
    }
}

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

import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.JitterClock;

import com.addthis.basis.util.Strings;
import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryChannelException;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;

import com.google.common.collect.ImmutableList;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;


import org.slf4j.LoggerFactory;
public class QueryChannelClient implements QuerySource {

    private static final Logger log = LoggerFactory.getLogger(QueryChannelClient.class);

    private static final Meter errorRate =
            Metrics.newMeter(QueryChannelClient.class, "errorRate",
                    "errorRate", TimeUnit.SECONDS);
    private static final Timer queryTimes = Metrics.newTimer(QueryChannelClient.class, "queryTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Gauge<Double> errorPercentage = Metrics.newGauge(QueryChannelClient.class, "errorPercentage",
            new Gauge<Double>() {
                public Double value() {
                    return errorRate.oneMinuteRate() / (errorRate.oneMinuteRate() + queryTimes.oneMinuteRate());
                }
            });


    private final Socket socket;
    private final QueryChannel<QueryChannelRequest, QueryChannelResponse> channel;
    private final Lock queryMapLock = new ReentrantLock();
    private final Map<Integer, ResultDelivery> queryMap = new HashMap<Integer, ResultDelivery>();
    private final AtomicInteger nextQueryID = new AtomicInteger(0);
    private final QueryHost host;
    private long lastPingReceivedTime = -1;
    private static final int TIMEOUT = 60 * 60000;
    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * connect to a query channel server
     */
    public QueryChannelClient(QueryHost host) throws IOException {
        this(host, 0);
    }

    /**
     * connect to a query channel server with optional channel flags
     */
    public QueryChannelClient(QueryHost host, int flags) throws IOException {
        try {
            this.host = host;
            this.socket = new Socket(host.getHost(), host.getPort());
            this.socket.setSoTimeout(TIMEOUT);
            this.channel = new QueryChannel<QueryChannelRequest, QueryChannelResponse>(socket, QueryChannelResponse.class, flags);
            new ChannelReader().start();
            //      new ChannelPinger().start();
        } catch (RuntimeException re) {
            // Downstream users of QueryChannelClient create one as
            // needed (ie to execute a query).  From their point of
            // view a failure to create a QueryChannelClient is a
            // query failure.
            errorRate.mark();
            throw re;
        } catch (IOException ioe) {
            errorRate.mark();
            throw ioe;
        }
    }

    public QueryChannelClient(String host, int port) throws IOException {
        this(new QueryHost(host, port));
    }

    @Override
    public String toString() {
        return "QueryChannelClient(" + host + ")";
    }

    public QueryHost getHost() {
        return host;
    }

    /**
     * terminate socket connection and all queries
     */
    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("closing " + this);
        }
        closed.set(true);
        try {
            /** cancel running queries */
            queryMapLock.lock();
            try {
                for (Integer qid : queryMap.keySet()) {
                    cancelQuery(qid, "closing");
                }
            } finally {
                queryMapLock.unlock();
            }
            channel.close();
            socket.close();
        } catch (IOException e)  {
            log.warn("", e);
        }
    }

    private boolean shutdownIfIdle() {
        queryMapLock.lock();
        try {
            if (queryMap.isEmpty()) {
                shutdownQueryChannelClient();
                return true;
            }
            return false;
        } finally {
            queryMapLock.unlock();
        }
    }

    /**
     * register query stream response endpoint
     */
    private void registerDelivery(Integer queryID, ResultDelivery delivery) {
        queryMapLock.lock();
        try {
            queryMap.put(queryID, delivery);
        } finally {
            queryMapLock.unlock();
        }
    }

    /**
     * de-register query stream response endpoint
     */
    private void dropDelivery(Integer queryID) {
        queryMapLock.lock();
        try {
            queryMap.remove(queryID);
        } finally {
            queryMapLock.unlock();
        }
    }

    /**
     * retrieve query stream response endpoint
     */
    private ResultDelivery getDelivery(Integer queryID) {
        queryMapLock.lock();
        try {
            return queryMap.get(queryID);
        } finally {
            queryMapLock.unlock();
        }
    }

    /**
     * @param query
     * @param consumer
     * @return
     * @throws Exception
     */
    @Override
    public QueryHandle query(Query query, DataChannelOutput consumer) throws QueryException {
        final Integer id = nextQueryID.incrementAndGet();
        registerDelivery(id, new ResultDelivery(query, id, consumer));

        if (closed.get()) {
            cancelQuery(id, "dead channel for host:" + host);
            dropDelivery(id);
            throw new RuntimeException("dead channel for host: " + host);
        }
        try {
            channel.sendSync(new QueryChannelRequest().setQuery(query).setQueryID(id));
        } catch (Exception e) {
            cancelQuery(id, e.getMessage());
            dropDelivery(id);
            throw new QueryChannelException(e);
        }

        return new QueryHandle() {
            @Override
            public void cancel(String message) {
                cancelQuery(id, message);
            }
        };
    }

    /** */
    private void cancelQuery(Integer id, String message) {
        if (id != null && !closed.get()) {
            try {
                channel.sendSync(new QueryChannelRequest().setQueryID(id).cancel());
            } catch (SocketException se) {
                if (log.isDebugEnabled()) {
                    log.debug("SocketException canceling host " + host + " query " + id + " " + se.getMessage() + " reason: " + message);
                }
            } catch (Exception e) {
                throw new QueryChannelException(e);
            }
        }
    }

    /**
     * simple streaming response
     */
    private class ResultDelivery {

        private final ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
        private final FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();
        private final DataChannelOutput consumer;
        private Integer queryID;
        private Query query;
        private final long start;

        ResultDelivery(Query query, Integer queryID, DataChannelOutput consumer) {
            this.query = query;
            this.queryID = queryID;
            this.consumer = consumer;
            this.start = System.currentTimeMillis();
        }

        public void handle(QueryChannelResponse response) throws Exception {
            if (response.isError()) {
                consumer.sourceError(new QueryException(response.getError()));
                errorRate.mark();
                dropDelivery(queryID);
                return;
            }
            try {
                if (response.isEnd()) {
                    consumer.sendComplete();
                    dropDelivery(queryID);
                    long dur = System.currentTimeMillis() - start;
                    queryTimes.update(dur, TimeUnit.MILLISECONDS);
                    return;
                }
            } catch (Exception ex) {
                log.warn("query op execution exception on " + query.uuid() + " host " + host + " err " + ex.getMessage());
                consumer.sourceError(DataChannelError.promote(ex));
                cancelQuery(queryID, ex.getMessage());
                dropDelivery(queryID);
            }
            try {
                int index = 0;
                Bundle bundle = response.getRow(consumer.createBundle(), fieldMap, classMap, index++);
                while (bundle != null) {
                    consumer.send(bundle);
                    bundle = response.getRow(consumer.createBundle(), fieldMap, classMap, index++);
                }

            } catch (Exception ex) {
                log.warn("consumer canceled " + query.uuid() + " channel id " + queryID + " host " + host + " err " + ex.getMessage());
                consumer.sourceError(DataChannelError.promote(ex));
                cancelQuery(queryID, ex.getMessage());
                dropDelivery(queryID);
            }
        }
    }

    private void shutdownQueryChannelClient() {
        shutdownQueryChannelClient(null);
    }

    private void shutdownQueryChannelClient(Exception e) {
        ImmutableList<ResultDelivery> resultDeliveryCollection;
        queryMapLock.lock();
        try {
            resultDeliveryCollection = new ImmutableList.Builder<ResultDelivery>().addAll(queryMap.values()).build();
            queryMap.clear();
        } finally {
            queryMapLock.unlock();
        }
        for (ResultDelivery delivery : resultDeliveryCollection) {
            try {
                delivery.handle(new QueryChannelResponse().setError("Query " + Strings.join(delivery.query.getPaths(), "|") + "\n\tFailed on " + host + " due to " + (e == null ? "" : e.getMessage())));
            } catch (Exception e1)  {
                log.warn("", e1);
            }
        }
        close();
    }

    /**
     * one reader per client
     */
    private class ChannelReader extends Thread {

        ChannelReader() {
            setName("Channel Reader @" + socket);
            setDaemon(true);
        }

        public void run() {
            while (true) {
                QueryChannelResponse response;
                try {
                    response = channel.recvSync();

                    if (response.isPing()) {
                        lastPingReceivedTime = JitterClock.globalTime();
                        continue;
                    }
                } catch (Exception e) {
                    if (log.isDebugEnabled()) {
                        log.debug("channel reader " + host + " exited: " + e);
                    }
                    shutdownQueryChannelClient(e);
                    break;
                }
                try {
                    ResultDelivery delivery = getDelivery(response.getQueryID());
                    if (delivery == null) {
                        final String errorMsg = "dead receiver for host " + host + ", channel id " + response.getQueryID() + " --> " + CodecJSON.encodeString(response);
                        log.warn(errorMsg);
                        /* drop for dead receiver */
                        shutdownQueryChannelClient(new QueryChannelException(errorMsg));
                        break;
                    }
                    delivery.handle(response);
                } catch (Exception ex) {
                    if (log.isDebugEnabled()) log.debug("channel reader " + host + " failed to handle message", ex);
                    }
            }
        }
    }

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }
}

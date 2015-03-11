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

import java.io.Closeable;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.engine.QueryEngine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/** */
public abstract class QueryEngineSource implements QuerySource, Closeable {

    private static final int maxConcurrency = Parameter.intValue("query.engine.source.maxConcurrency", 4);

    private final Logger log = LoggerFactory.getLogger(QueryEngineSource.class);

    @SuppressWarnings("unused")
    private final Gauge<Integer> engineGatePermitMetric = Metrics.newGauge(QueryEngineSource.class,
                                                                           "engineGatePermitMetric",
                                                                           new Gauge<Integer>() {
        @Override
        public Integer value() {
            return (maxConcurrency - executor.getActiveCount());
        }
    });

    private final Histogram engineGateHistogram = Metrics.newHistogram(QueryEngineSource.class, "engineGateHistogram");

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(maxConcurrency, maxConcurrency,
                                                                       0L, TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new ThreadFactoryBuilder()
                                                                               .setNameFormat("QueryEngineSource-%d")
                                                                               .setDaemon(true)
                                                                               .build());

    private final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public QueryHandle query(final Query query, final DataChannelOutput consumer) throws QueryException {
        Handle result = new Handle(query, consumer);
        result.setFuture(executor.submit(result));
        return result;
    }

    public abstract QueryEngine getEngineLease();

    /** */
    public class Handle implements Runnable, QueryHandle {

        private final Query query;
        private final DataChannelOutput consumer;
        private Future<?> future;

        Handle(Query query, DataChannelOutput consumer) {
            this.query = query;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            QueryEngine engine = null;
            try {
                engineGateHistogram.update(maxConcurrency - executor.getActiveCount());
                engine = getEngineLease();
                engine.search(query, consumer,
                        new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE));
                consumer.sendComplete();
            } catch (QueryException e) {
                log.warn("query exception " + query.uuid() + " " + e + " " + consumer);
                consumer.sourceError(e);
            } catch (Exception e) {
                log.warn("query error " + query.uuid() + " " + e + " " + consumer, e);
                consumer.sourceError(new QueryException(e));
            } finally {
                engineGateHistogram.update(maxConcurrency - executor.getActiveCount());
                if (engine != null) {
                    try {
                        engine.release();
                    } catch (Throwable t) {
                        log.warn("[dispatch] error during db release of " + engine + " : " + t, t);
                    }
                }
            }
        }

        @Override
        public void cancel(String message) {
            if (future != null) {
                log.warn(query.uuid() + " cancel called on handle " + consumer + " message: " + message);
                future.cancel(true);
            }
        }

        void setFuture(Future<?> future) {
            this.future = future;
        }
    }

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if(!closed.getAndSet(true)) {
            executor.shutdownNow();
        }
    }
}

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
package com.addthis.hydra.query;

import java.util.concurrent.Semaphore;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/** */
public abstract class QueryEngineSource implements QuerySource {

    private static int maxConcurrency = Parameter.intValue("query.engine.source.maxConcurrency", 4);

    private final Logger log = LoggerFactory.getLogger(QueryEngineSource.class);
    private final Semaphore engineGate = new Semaphore(maxConcurrency);

    private final Gauge<Integer> engineGatePermitMetric = Metrics.newGauge(QueryEngineSource.class, "engineGatePermitMetric", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return engineGate.availablePermits();
        }
    });
    private final Histogram engineGateHistogram = Metrics.newHistogram(QueryEngineSource.class, "engineGateHistogram");

    @Override
    public QueryHandle query(final Query query, final DataChannelOutput consumer) throws QueryException {
        return new Handle(query, consumer);
    }

    public abstract QueryEngine getEngineLease();

    /** */
    private class Handle extends Thread implements QueryHandle {

        private Query query;
        private QueryEngine engine;
        private DataChannelOutput consumer;

        Handle(Query query, DataChannelOutput consumer) {
            setName("EngineSource " + query.uuid());
            this.query = query;
            this.consumer = consumer;
            start();
        }

        @Override public void run() {
            engine = null;
            try {
                engineGate.acquire(1);
                engineGateHistogram.update(engineGate.availablePermits());
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
                engineGate.release();
                engineGateHistogram.update(engineGate.availablePermits());
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
            log.warn(query.uuid() + " cancel called on handle " + consumer + " message: " + message);
            if (engine != null) {
                interrupt();
            }
        }
    }

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}

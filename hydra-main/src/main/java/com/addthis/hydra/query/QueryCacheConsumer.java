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

import java.io.EOFException;
import java.io.IOException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.muxy.MuxFile;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class QueryCacheConsumer implements QuerySource, BundleFactory {

    private static Logger log = LoggerFactory.getLogger(QueryCacheConsumer.class);

    private final MuxFile cacheData;
    private final KVBundleFormat format;
    private AtomicBoolean canceled = new AtomicBoolean(false);
    private final AtomicInteger cacheConsumerThreads = new AtomicInteger();

    public QueryCacheConsumer(MuxFile cacheData) {
        this.cacheData = cacheData;
        this.format = new KVBundleFormat();
    }

    @Override
    public QueryHandle query(final Query query, DataChannelOutput consumer) throws QueryException {
        new CacheConsumerThread(query, consumer);
        return new QueryHandle() {
            @Override
            public void cancel(String message) {
                if (log.isDebugEnabled()) log.debug(query.uuid() + " canceled due to: " + message);
                canceled.set(true);
            }
        };

    }

    @Override
    public void noop() {
    }

    @Override
    public boolean isClosed() {
        return canceled.get();
    }

    @Override
    public Bundle createBundle() {
        return new KVBundle(format);
    }

    private class CacheConsumerThread extends Thread {

        private final Query query;
        private final DataChannelOutput consumer;

        public CacheConsumerThread(Query query, DataChannelOutput consumer) {
            this.query = query;
            this.consumer = consumer;
            setDaemon(true);
            setName("CacheConsumerThread-" + cacheConsumerThreads.incrementAndGet());
            start();
        }

        @Override
        public void run() {
            if (log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Starting to consume query: " + query.uuid() + " from cache");
            }
            // we can ignore the query because the data is already cached...
            try {
                DataChannelReader dataChannelReader = new DataChannelReader(QueryCacheConsumer.this, cacheData.read(0));
                Bundle bundle;
                while (!canceled.get() && (bundle = dataChannelReader.read()) != null) {
                    consumer.send(bundle);
                }
            } catch (EOFException eof) {
                // thrown by DataChannelReader when end of stream is reached
                if (!canceled.get()) {
                    consumer.sendComplete();
                    if (log.isDebugEnabled() || query.isTraced()) {
                        Query.emitTrace("Completed consuming query: " + query.uuid() + " from cache");
                    }
                }
            } catch (IOException e)  {
                log.warn("", "IO Exception reading bundle from cache");
                consumer.sourceError(new DataChannelError(e));
            } catch (Exception e)  {
                log.warn("", "Generic or Wrapped Exception reading bundle from cache");
                consumer.sourceError(new DataChannelError(e));
            }
        }
    }
}

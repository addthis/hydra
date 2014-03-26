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
package com.addthis.hydra.data.query.engine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.test.SlowTest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SlowTest.class)
public class QueryEngineCacheTest {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineCacheTest.class);

    @Test
    public void stayUnderCacheMax() throws Exception {
        AtomicInteger oe = new AtomicInteger(0);
        QueryEngineTestCache cache = new QueryEngineTestCache(oe);
        ExecutorService dummySearchService = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            dummySearchService.execute(new DummySearcher(cache));
        }
        for (int i = 0; i < 100; i++) {
            log.debug(cache.status());
            Assert.assertTrue("Engine cache grew too large at :" + oe.get(), oe.get() <= 30);
            Thread.sleep(250);
        }
        dummySearchService.shutdownNow();
    }

    static class DummySearcher implements Runnable {

        QueryEngineTestCache cache;

        public DummySearcher(QueryEngineTestCache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    //get the engine we need
                    QueryEngine qe = cache.getAndLease(String.valueOf((int) (Math.random() * 30)));
                    //search
                    Thread.sleep(500 + (int) (Math.random() * 500));
                    //release engine
                    qe.release();
                } catch (InterruptedException ignored) {
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class QueryEngineTestCache extends QueryEngineCache {

        static final int engineCacheSize = 25;
        static final int refreshInterval = 1;
        static final int failInterval = 120;
        private final AtomicInteger oe;

        public QueryEngineTestCache(AtomicInteger oe) {
            super(engineCacheSize, refreshInterval, failInterval, 0, new DummyEngineLoader(oe));
            this.oe = oe;
        }

        public String status() {
            return "status: newOpenedEngines=" + EngineLoader.newEnginesOpened.count() +
                   " enginesRefreshed=" + RefreshEngineCall.enginesRefreshed.count() +
                   " openEngines=" + oe + " evictedDirectories=" + EngineRemovalListener.directoriesEvicted.count();
        }
    }

    static class DummyEngineLoader extends EngineLoader {

        private final AtomicInteger oe;

        public DummyEngineLoader(AtomicInteger oe) {
            this.oe = oe;
        }

        @Override
        protected QueryEngine newQueryEngineDirectory(String dir) throws Exception {
            return new DummyEngine(oe);
        }
    }

    static class DummyEngine extends QueryEngineDirectory {

        public AtomicInteger oe;

        public DummyEngine(AtomicInteger oe) {
            super(null, null);
            this.oe = oe;
            oe.incrementAndGet();
        }

        @Override
        public void loadAllFrom(QueryEngineDirectory other) {
            //do nothing
        }

        @Override
        public void init() {

        }

        @Override
        public boolean isOlder(String dir) {
            return Math.random() < 0.25;
        }

        @Override
        public void close() {
            oe.decrementAndGet();
        }
    }
}

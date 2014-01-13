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
package com.addthis.hydra.data.query;

import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.test.SlowTest;

import com.addthis.hydra.data.query.QueryEngine;
import com.addthis.hydra.data.query.QueryEngineCache;
import com.addthis.hydra.data.query.QueryEngineDirectory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class TestQueryEngineCache {

    @Test
    public void test() throws Exception {
        QueryEngineTestCache cache = new QueryEngineTestCache();
        for (int i = 0; i < 5; i++) {
            (new DummySearcher(cache)).start();
        }
        for (int i = 0; i < 100; i++) {
//			System.out.println(cache.status());
            Assert.assertTrue("Engine cache grew too large at :" + cache.oe.get(), cache.oe.get() <= 30);
            Thread.sleep(250);
        }
    }

    class DummySearcher extends Thread {

        QueryEngineTestCache cache;

        public DummySearcher(QueryEngineTestCache cache) {
            this.cache = cache;
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    //get the engine we need
                    QueryEngine qe = cache.getAndLease("" + (int) (Math.random() * 30));
                    //search
                    Thread.sleep(500 + (int) (Math.random() * 500));
                    //release engine
                    qe.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class QueryEngineTestCache extends QueryEngineCache {

        static final int engineCacheSize = 25;
        static final int refreshInterval = 1;
        static final int failInterval = 120;
        public AtomicInteger oe = new AtomicInteger(0);

        public QueryEngineTestCache() {
            super(engineCacheSize, refreshInterval, failInterval);
        }

        public String status() {
            return "status: newOpenedEngines=" + newEnginesOpened.count() + " enginesRefreshed=" + enginesRefreshed.count() + " openEngines=" + oe;
        }

        @Override
        protected QueryEngine createEngine(String dir) throws Exception {
            return new DummyEngine(oe);
        }
    }

    class DummyEngine extends QueryEngineDirectory {

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

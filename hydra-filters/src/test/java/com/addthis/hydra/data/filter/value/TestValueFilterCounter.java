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
package com.addthis.hydra.data.filter.value;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The motivation for the concurrency test is to ensure
 * that unique values are generated in the following Hydra job.
 * <p/>
 * <pre>{
 *   type : "map",
 *   source : {
 *     type : "empty",
 *     maxPackets : 1000000,
 *   },
 *   map : {
 *     fields : [],
 *     filterOut :
 *     {op : "map", fields:[
 *       {from : "counter", filter: {op:"count", format:"0000000"}},
 *       {from: "DATE_YMD", filter: {op: "time-range", now:true, format:"YYMMdd"}},
 *     ]},
 *   },
 *   output : {
 *     type : "file",
 *     path : ["{{DATE_YMD}}", "/", "[[node]]"],
 *     writer : {
 *       maxOpen : 1024,
 *       flags : {
 *         dir : "split",
 *         multiplex : true,
 *       },
 *       format : {
 *         type : "channel"
 *       }
 *     }
 *   }
 * }</pre>
 */
public class TestValueFilterCounter {

    public class CounterThread extends Thread {

        final ValueFilterCounter counter;
        final int iterations;

        public CounterThread(ValueFilterCounter counter, int iterations) {
            this.counter = counter;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            for (int i = 0; i < iterations; i++) {
                counter.filterValue(null);
            }

        }

    }


    @Test
    public void singleThreadedCounter() {
        ValueFilterCounter counter = new ValueFilterCounter();
        Thread thread = new CounterThread(counter, 1000000);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            assertTrue(false);
        }
        Long expected = 1000000l;
        Long observed = counter.filterValue(null).asLong().getLong().longValue();
        assertEquals(expected, observed);
    }

    @Test
    public void multiThreadedCounter() {
        ValueFilterCounter counter = new ValueFilterCounter();
        Thread thread1 = new CounterThread(counter, 1000000);
        Thread thread2 = new CounterThread(counter, 1000000);
        Thread thread3 = new CounterThread(counter, 1000000);
        Thread thread4 = new CounterThread(counter, 1000000);
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        try {
            thread1.join();
            thread2.join();
            thread3.join();
            thread4.join();
        } catch (InterruptedException e) {
            assertTrue(false);
        }
        Long expected = 4000000l;
        Long observed = counter.filterValue(null).asLong().getLong().longValue();
        assertEquals(expected, observed);
    }
}

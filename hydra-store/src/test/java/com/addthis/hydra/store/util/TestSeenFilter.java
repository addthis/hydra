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
package com.addthis.hydra.store.util;

import java.util.HashSet;
import java.util.Random;

import java.text.DecimalFormat;

import com.addthis.basis.test.SlowTest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class TestSeenFilter {

    private static final Random rand = new Random(8675309);
    private static final DecimalFormat format1 = new DecimalFormat("##.##%");
    private static final DecimalFormat format2 = new DecimalFormat("0,000");

    @Test
    public void testSeenFilterGhetto() {
        testSeenFilter(new SeenFilterBasic<Long>(40000, 3, 0), 10000);
        testSeenFilter(new SeenFilterBasic<Long>(40000, 3, 1), 10000);
        testSeenFilter(new SeenFilterBasic<Long>(40000, 3, 2), 10000);
        testSeenFilter(new SeenFilterBasic<Long>(40000, 3, 3), 10000);
        testSeenFilter(new SeenFilterBasic<Long>(40000, 3, 4), 10000);
    }

    private void testSeenFilter(SeenFilter<Long> filter, int capacity) {
        long time = System.nanoTime();
        int incr = capacity / 10;
        for (int i = incr; i <= capacity; i += incr) {
            filter.clear();
            HashSet<Long> incSet = new HashSet<>();
            for (int j = 0; j < i; j++) {
                Long next = rand.nextLong();
                incSet.add(next);
                filter.setSeen(next);
            }
            for (Long l : incSet) {
                Assert.assertTrue(filter.getSeen(l));
            }
            int error = 0;
            for (long j = 0; j < capacity; j++) {
                if (!incSet.contains(j) && filter.getSeen(j)) {
                    error++;
                }
            }
            double errRate = ((error * 1.0d) / (capacity * 1.0d));
            System.out.println("cap " + capacity + " tested to " + i + " with " + error + " errors or " + format1.format(errRate) + " false positive @ " + ((i * 100) / capacity) + "% load for " + filter);
        }
        System.out.println("---------( test time " + format2.format(System.nanoTime() - time) + " ns )---------");
    }
}

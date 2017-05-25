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
package com.addthis.hydra.data.util;

import org.junit.Assert;
import org.junit.Test;

public class TestKeyPercentileDistribution {

    @Test
    public void stats() {
        KeyPercentileDistribution distribution = new KeyPercentileDistribution(10).init();
        distribution.update(1);
        distribution.update(2);
        distribution.update(2);
        distribution.update(3);

        Assert.assertEquals(4, distribution.count());
        Assert.assertEquals(Math.sqrt(0.666666666666666), distribution.stdDev(), 10E-15);
        Assert.assertEquals(1, distribution.min());
        Assert.assertEquals(3, distribution.max());
    }

    @Test
    public void lessDataPointsThanSampleSize() {
        KeyPercentileDistribution distribution = new KeyPercentileDistribution(100).init();
        for (int i = 0; i < 50; i++) {
            distribution.update(i);
        }

        Assert.assertEquals(50, distribution.getSnapshot().size());
    }

    @Test
    public void moreDataPointsThanSampleSize() {
        KeyPercentileDistribution distribution = new KeyPercentileDistribution(10).init();
        for (int i = 0; i < 15; i++) {
            distribution.update(i);
        }

        Assert.assertEquals(10, distribution.getSnapshot().size());
    }
}

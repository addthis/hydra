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

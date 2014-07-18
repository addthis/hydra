package com.addthis.hydra.data.tree.prop;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataCountingMinTest {

    @Test
    public void testCountMin() {
        DataCountingMin.Config config = new DataCountingMin.Config();
        config.percentage = 0.01;
        DataCountingMin countingMin = config.newInstance();
        countingMin.add("a", 3);
        assertEquals(0, countingMin.count());
        countingMin.add("a", 3);
        assertEquals(0, countingMin.count());
        countingMin.add("a", 3);
        assertEquals(1, countingMin.count());
        countingMin.add("a", 3);
        assertEquals(1, countingMin.count());
        countingMin.add("b", 2);
        assertEquals(1, countingMin.count());
        countingMin.add("b", 2);
        assertEquals(2, countingMin.count());
        countingMin.add("b", 2);
        assertEquals(2, countingMin.count());
    }

}

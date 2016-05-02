package com.addthis.hydra.task.output;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PartitionDataTest {

    @Test
    public void getReplacement_noPart() throws Exception {
        PartitionData result = PartitionData.getPartitionData("foo");
        assertNotNull(result);
        assertNull(result.getReplacementString());
        assertEquals(3, result.getPadTo());
    }

    @Test
    public void getReplacement_withPart() throws Exception {
        PartitionData result = PartitionData.getPartitionData("foo{{PART:10}}");
        assertNotNull(result);
        assertEquals("{{PART:10}}", result.getReplacementString());
        assertEquals(10, result.getPadTo());
    }

}

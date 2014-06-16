package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.hydra.data.tree.ReadNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataCountMinSketchTest {

    @Test
    public void testCms() {
        DataCountMinSketch dataCountMinSketch = new DataCountMinSketch(10,100_000);
        dataCountMinSketch.add("a", 5);
        dataCountMinSketch.add("b", 2);
        dataCountMinSketch.add("a", 7);
        dataCountMinSketch.add("c", 3);

        DataCountMinSketch dataCountMinSketch2 = new DataCountMinSketch(10,100_000);
        dataCountMinSketch2.add("c", 4);
        dataCountMinSketch2.add("d", 2);
        dataCountMinSketch2.add("c", 1);

        List<ReadNode> aggregatedNodes = new ArrayList<>();
        aggregatedNodes.addAll(dataCountMinSketch.getNodes(null, "b~c"));
        aggregatedNodes.addAll(dataCountMinSketch2.getNodes(null, "b~c"));
        Map<String, Long> aggregatedCount = new HashMap<>();
        for (ReadNode node : aggregatedNodes) {
            String key = node.getName();
            long count = node.getCounter();
            if (aggregatedCount.containsKey(key)) {
                aggregatedCount.put(key, count + aggregatedCount.get(key));
            } else {
                aggregatedCount.put(key, count);
            }
        }
        assertEquals("should get correct total for key in one cms", 2, aggregatedCount.get("b").longValue());
        assertEquals("should get correct total for key two two cmses", 8, aggregatedCount.get("c").longValue());
    }

}

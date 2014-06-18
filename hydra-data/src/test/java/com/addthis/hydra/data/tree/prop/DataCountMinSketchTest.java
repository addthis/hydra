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
package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.hydra.data.tree.DataTreeNode;

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

        List<DataTreeNode> aggregatedNodes = new ArrayList<>();
        aggregatedNodes.addAll(dataCountMinSketch.getNodes(null, "b~c"));
        aggregatedNodes.addAll(dataCountMinSketch2.getNodes(null, "b~c"));
        Map<String, Long> aggregatedCount = new HashMap<>();
        for (DataTreeNode node : aggregatedNodes) {
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

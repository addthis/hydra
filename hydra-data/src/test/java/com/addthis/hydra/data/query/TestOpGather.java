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

import org.junit.Test;

public class TestOpGather extends TestOp {

    @Test
    public void testGather() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3"),
                "gather=ksssu",
                new DataTableHelper().
                        tr().td("b", "2", "4", "6", "2").
                        tr().td("c", "2", "4", "6", "2").
                        tr().td("a", "3", "6", "9", "3")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "5", "8", "3").
                        tr().td("c", "8", "2", "4").
                        tr().td("a", "1", "7", "3").
                        tr().td("b", "3", "2", "3").
                        tr().td("c", "1", "2", "5").
                        tr().td("a", "1", "2", "3"),
                "gather=kaaa",
                new DataTableHelper().
                        tr().td("b", "4", "5", "3").
                        tr().td("c", "4", "2", "4").
                        tr().td("a", "1", "3", "3")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1").
                        tr().td("b", "5", "8", "3").
                        tr().td("c").td().td("2", "4").
                        tr().td("a", "1", "7", "3").
                        tr().td("b", "3").td().td("3").
                        tr().td("c", "1", "2").
                        tr().td("a", "1", "2", "3"),
                "gather=ksss",
                new DataTableHelper().
                        tr().td("b", "8", "8", "6").
                        tr().td("c", "1", "4", "4").
                        tr().td("a", "3", "9", "6")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3"),
                "gather=kiss",
                new DataTableHelper().
                        tr().td("b", "4", "6").
                        tr().td("c", "4", "6").
                        tr().td("a", "6", "9")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "x").
                        tr().td("a", "y").
                        tr().td("b", "x").
                        tr().td("b", "z"),
                "gather=kj;sort",
                new DataTableHelper().
                        tr().td("a", "x,y").
                        tr().td("b", "x,z")
        );
    }

    @Test
    public void testGatherWhenFallToDisk() throws Exception {
        System.setProperty("opgather.tiptodisk", "true");

        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3"),
                "gather=ksssu",
                new DataTableHelper().
                        tr().td("a", "3", "6", "9", "3").
                        tr().td("b", "2", "4", "6", "2").
                        tr().td("c", "2", "4", "6", "2"),
                2, 2
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "5", "8", "3").
                        tr().td("c", "8", "2", "4").
                        tr().td("a", "1", "7", "3").
                        tr().td("b", "3", "2", "3").
                        tr().td("c", "1", "2", "5").
                        tr().td("a", "1", "2", "3"),
                "gather=kaaa",
                new DataTableHelper().
                        tr().td("a", "1", "3", "3").
                        tr().td("b", "4", "5", "3").
                        tr().td("c", "4", "2", "4"),
                2, 2
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1").
                        tr().td("b", "5", "8", "3").
                        tr().td("c").td().td("2", "4").
                        tr().td("a", "1", "7", "3").
                        tr().td("b", "3").td().td("3").
                        tr().td("c", "1", "2").
                        tr().td("a", "1", "2", "3"),
                "gather=ksss",
                new DataTableHelper().
                        tr().td("a", "3", "9", "6").
                        tr().td("b", "8", "8", "6").
                        tr().td("c", "1", "4", "4"),
                2, 2
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3").
                        tr().td("a", "1", "2", "3"),
                "gather=kiss",
                new DataTableHelper().
                        tr().td("a", "6", "9").
                        tr().td("b", "4", "6").
                        tr().td("c", "4", "6"),
                2, 2
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "x").
                        tr().td("a", "y").
                        tr().td("b", "x").
                        tr().td("b", "z"),
                "gather=kj;sort",
                new DataTableHelper().
                        tr().td("a", "x,y").
                        tr().td("b", "x,z"),
                2, 2
        );
    }

    //@Test
    public void comparePerformance() throws Exception {
        long inMemoryTime = 0;
        long onDiskTime = 0;

        for (int i = 0; i != 100; i++) {
            inMemoryTime -= System.currentTimeMillis();
            testGather();
            inMemoryTime += System.currentTimeMillis();
        }

        for (int i = 0; i != 100; i++) {
            onDiskTime -= System.currentTimeMillis();
            testGatherWhenFallToDisk();
            onDiskTime += System.currentTimeMillis();
        }

        System.out.println("InMemoryTime:" + inMemoryTime + " onDiskTime:" + onDiskTime);
    }
}

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

public class TestOpMerge extends TestOp {

    @Test
    public void testMerge() throws Exception {
        DataTableHelper t1 = new DataTableHelper().
                tr().td("cat", "a", "1").
                tr().td("cat", "b", "2").
                tr().td("cat", "b", "3").
                tr().td("cat", "a", "3").
                tr().td("dog", "a", "6").
                tr().td("dog", "a", "8").
                tr().td("dog", "b", "7").
                tr().td("dog", "b", "8");
        doOpTest(t1, "merge=kisu",
                new DataTableHelper().
                        tr().td("cat", "9", "4").
                        tr().td("dog", "29", "4"), 3);
    }

    @Test
    public void testMergeLast() throws Exception {
        DataTableHelper t1 = new DataTableHelper().
                tr().td("cat", "1").
                tr().td("cat", "2").
                tr().td("cat", "3").
                tr().td("cat", "4");

        doOpTest(t1, "merge=ls,1",
                new DataTableHelper().
                        tr().td("cat", "1").
                        tr().td("cat", "2").
                        tr().td("cat", "3").
                        tr().td("cat", "4"), 2);
    }

    @Test
    public void testMergeLast2() throws Exception {
        DataTableHelper t1 = new DataTableHelper().
                tr().td("cat", "1").
                tr().td("cat", "2").
                tr().td("cat", "3").
                tr().td("cat", "4");

        doOpTest(t1, "merge=ls,2",
                new DataTableHelper().
                        tr().td("cat", "3").
                        tr().td("cat", "7"), 2);
    }

    @Test
    public void testMerge2() throws Exception {
        DataTableHelper t1 = new DataTableHelper().
                tr().td("cat", "a", "1").
                tr().td("cat", "b", "2").
                tr().td("cat", "b", "3").
                tr().td("cat", "a", "3").
                tr().td("dog", "a", "6").
                tr().td("dog", "a", "8").
                tr().td("dog", "b", "7").
                tr().td("dog", "b", "8");
        doOpTest(t1, "merge=kksu",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "1").
                        tr().td("cat", "b", "5", "2").
                        tr().td("cat", "a", "3", "1").
                        tr().td("dog", "a", "14", "2").
                        tr().td("dog", "b", "15", "2"), 4);
    }

    @Test
    public void testMergeM() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "2").
                        tr().td("cat", "a", "3").
                        tr().td("cat", "b", "3").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "b", "7").
                        tr().td("dog", "a", "8").
                        tr().td("dog", "b", "8"),
                "merge=kiMu",
                new DataTableHelper().
                        tr().td("cat", "3", "4").
                        tr().td("dog", "8", "4")
        );
    }

    @Test
    public void testMergeSumM() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "2").
                        tr().td("cat", "a", "3").
                        tr().td("cat", "b", "3").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "b", "7").
                        tr().td("dog", "a", "8").
                        tr().td("dog", "b", "8"),
                "sum=2;merge=kiMsu",
                new DataTableHelper().
                        tr().td("cat", "3", "19", "4").
                        tr().td("dog", "8", "105", "4")
        );
    }

    @Test
    public void testMergeJoin() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "2").
                        tr().td("cat", "a", "3").
                        tr().td("cat", "b", "3").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "b", "7").
                        tr().td("dog", "a", "8").
                        tr().td("dog", "b", "8"),
                "sum=2;merge=kij",
                new DataTableHelper().
                        tr().td("cat", "1,2,3,3").
                        tr().td("dog", "6,7,8,8"), 2
        );
    }
}

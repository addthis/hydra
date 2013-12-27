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

public class TestOpRoll extends TestOp {

    @Test
    public void testRoll() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("cat", "b", "2", "3").
                        tr().td("cat", "c", "3", "4").
                        tr().td("dog", "a", "6", "0").
                        tr().td("dog", "b", "7", "1").
                        tr().td("dog", "c", "8", "0"),
                "sum=2,3",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", "1", "2").
                        tr().td("cat", "b", "2", "3", "3", "5").
                        tr().td("cat", "c", "3", "4", "6", "9").
                        tr().td("dog", "a", "6", "0", "12", "9").
                        tr().td("dog", "b", "7", "1", "19", "10").
                        tr().td("dog", "c", "8", "0", "27", "10")
        );
    }

    @Test
    public void testRollColon() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("cat", "b", "1", "3").
                        tr().td("cat", "c", "1", "4").
                        tr().td("dog", "a", "1", "0").
                        tr().td("dog", "b", "2", "1").
                        tr().td("dog", "c", "2", "2"),
                "min=3:2",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", "2").
                        tr().td("cat", "b", "1", "3", "2").
                        tr().td("cat", "c", "1", "4", "2").
                        tr().td("dog", "a", "1", "0", "0").
                        tr().td("dog", "b", "2", "1", "1").
                        tr().td("dog", "c", "2", "2", "1")
        );
    }


    @Test
    public void testRollFill() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("cat", "b", "2", "3").
                        tr().td("cat", "c", "3", "4", "3").
                        tr().td("dog", "a", "6", "0").
                        tr().td("dog", "b", "7", "1", "2").
                        tr().td("dog", "c", "8", "0"),
                "sum=2,3,4",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", null).td("1", "2", "0").
                        tr().td("cat", "b", "2", "3", null).td("3", "5", "0").
                        tr().td("cat", "c", "3", "4", "3").td("6", "9", "3").
                        tr().td("dog", "a", "6", "0", null).td("12", "9", "3").
                        tr().td("dog", "b", "7", "1", "2").td("19", "10", "5").
                        tr().td("dog", "c", "8", "0", null).td("27", "10", "5")
        );
    }

    @Test
    public void testRollInPlace() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("cat", "b", "2", "3").
                        tr().td("cat", "c", "3", "4").
                        tr().td("dog", "a", "6", "0").
                        tr().td("dog", "b", "7", "1").
                        tr().td("dog", "c", "8", "0"),
                "sum=s2,3",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("cat", "b", "3", "5").
                        tr().td("cat", "c", "6", "9").
                        tr().td("dog", "a", "12", "9").
                        tr().td("dog", "b", "19", "10").
                        tr().td("dog", "c", "27", "10")
        );
    }
}

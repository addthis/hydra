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

public class TestOpTop extends TestOp {

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
                "top=ksssu;sort",
                new DataTableHelper().
                        tr().td("a", "3", "6", "9", "3").
                        tr().td("b", "2", "4", "6", "2").
                        tr().td("c", "2", "4", "6", "2")
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
                "top=kaaa;sort",
                new DataTableHelper().
                        tr().td("a", "1", "3", "3").
                        tr().td("b", "4", "5", "3").
                        tr().td("c", "4", "2", "4")
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
                "top=ksss;sort",
                new DataTableHelper().
                        tr().td("a", "3", "9", "6").
                        tr().td("b", "8", "8", "6").
                        tr().td("c", "1", "4", "4")
        );
    }

    @Test
    public void testIgnore() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "5", "8", "3").
                        tr().td("c", "8", "2", "4").
                        tr().td("a", "1", "7", "3").
                        tr().td("b", "3", "2", "3").
                        tr().td("c", "1", "2", "5").
                        tr().td("a", "1", "2", "3"),
                "top=issi",
                new DataTableHelper().
                        tr().td("20", "25")
        );
    }

    @Test
    public void testTop() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("x", "1").
                        tr().td("a", "1").
                        tr().td("b", "2").
                        tr().td("c", "3").
                        tr().td("d", "9").
                        tr().td("a", "1").
                        tr().td("b", "2").
                        tr().td("c", "3").
                        tr().td("a", "1").
                        tr().td("y", "1"),
                "top=2,ktsu;sort",
                new DataTableHelper().
                        tr().td("c", "6", "2").
                        tr().td("d", "9", "1")
        );
    }
}

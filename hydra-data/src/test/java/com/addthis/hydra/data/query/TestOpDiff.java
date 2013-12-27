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

public class TestOpDiff extends TestOp {

    @Test
    public void testFold() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "2").
                        tr().td("cat", "a", "3").
                        tr().td("cat", "b", "3").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "b", "7").
                        tr().td("dog", "a", "8").
                        tr().td("dog", "b", "8").
                        tr().td("dog", "b", "9").
                        tr().td("dog", "b", "4").
                        tr().td("dog", "a", "2").
                        tr().td("dog", "b", "3").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "a", "8").
                        tr().td("foo", "a", "3").
                        tr().td("bar", "a", "3"),
                "diff=kad",
                new DataTableHelper().
                        tr().td("cat", "", "1").
                        tr().td("cat", "", "0").
                        tr().td("dog", "", "1").
                        tr().td("dog", "", "0").
                        tr().td("dog", "+", "9").
                        tr().td("dog", "+", "4").
                        tr().td("dog", "", "1").
                        tr().td("dog", "", "2").
                        tr().td("foo", "-", "-3")
        );
    }
}

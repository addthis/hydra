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

public class TestOpGroupBy extends TestOp {

    @Test
    public void testGroupBy() throws Exception {
        DataTableHelper t1 = new DataTableHelper().
                tr().td("cat", "a", "1").
                tr().td("cat", "b", "2").
                tr().td("cat", "b", "3").
                tr().td("cat", "a", "3").
                tr().td("dog", "a", "6").
                tr().td("dog", "a", "8").
                tr().td("dog", "b", "7").
                tr().td("dog", "b", "8");
        doOpTest(t1, "groupby=k:limit=2",
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "2").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "a", "8"),
                3);
        DataTableHelper t2 = new DataTableHelper().
                tr().td("cat", "a", "1").
                tr().td("cat", "b", "1").
                tr().td("cat", "b", "1").
                tr().td("dog", "a", "6").
                tr().td("dog", "a", "7").
                tr().td("dog", "b", "6").
                tr().td("dog", "c", "6");
        doOpTest(t2, "groupby=kik:limit=2",
                 new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("cat", "b", "1").
                        tr().td("dog", "a", "6").
                        tr().td("dog", "a", "7").
                        tr().td("dog", "b", "6"),
                 3);
    }

}

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

public class TestOpOrderMap extends TestOp {

    @Test
    public void testNoop() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0"),
                "ordermap=1,foo,2,3",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0")
        );
    }

    @Test
    public void testSimple() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", "", "").
                        tr().td("foo", "a", "1", "2", "", "").
                        tr().td("dog", "a", "6", "0", "", ""),
                "ordermap=0,foo,1,4",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", "", "").
                        tr().td("foo", "a", "1", "2", "a", "").
                        tr().td("dog", "a", "6", "0", "", "")
        );
    }


    @Test
    public void testOverWrite() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0"),
                "ordermap=0,cat,1,2;ordermap=0,cat,2,3;ordermap=0,dog,2,3",
                new DataTableHelper().
                        tr().td("cat", "a", "a", "a").
                        tr().td("dog", "a", "6", "6")
        );
    }


}

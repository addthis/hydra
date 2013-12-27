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

public class TestOpOrder extends TestOp {

    @Test
    public void testOrder() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0"),
                "order=1,2,0,3",
                new DataTableHelper().
                        tr().td("a", "1", "cat", "2").
                        tr().td("a", "6", "dog", "0")
        );
    }

    @Test
    public void testOrderDrop() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0"),
                "order=2,0,3",
                new DataTableHelper().
                        tr().td("1", "cat", "2").
                        tr().td("6", "dog", "0")
        );
    }
}

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


public class TestOpMap extends TestOp {

    @Test
    public void testMap() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2").
                        tr().td("b", "2", "3").
                        tr().td("c", "3", "4"),
                "map=0::{a:'x',b:'y',c:'z'}",
                new DataTableHelper().
                        tr().td("x", "1", "2").
                        tr().td("y", "2", "3").
                        tr().td("z", "3", "4")
        );
    }
}

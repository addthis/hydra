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

public class TestOpPivot extends TestOp {

    @Test
    public void useCellAndRowOps() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("x", "a", "1").
                        tr().td("y", "b", "2").
                        tr().td("z", "c", "3"),
                "pivot=1,0,2,sum,sum",
                new DataTableHelper().
                        tr().tdNull().td("x", "y", "z").
                        tr().td("a", "1", "0", "0", "1").
                        tr().td("b", "0", "2", "0", "2").
                        tr().td("c", "0", "0", "3", "3")
        );
    }

    @Test
    public void useNoOps() throws Exception {
        doOpTest(
                new DataTableHelper()
                        .tr().td("x", "a", "1")
                        .tr().td("y", "b", "2")
                        .tr().td("z", "c", "3"),
                "pivot=1,0,2",
                new DataTableHelper()
                        .tr().tdNull().td("x", "y", "z")
                        .tr().td("a", "1").tdNull().tdNull()
                        .tr().td("b").tdNull().td("2").tdNull()
                        .tr().td("c").tdNull().tdNull().td("3")
                );
    }
}

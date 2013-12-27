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


public class TestOpCompare extends TestOp {

    @Test
    public void testMap() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("SClub", "7").
                        tr().td("Maroon", "5").
                        tr().td("Fab", "4"),
                "compare=1:geq:5",
                new DataTableHelper().
                        tr().td("SClub", "7").
                        tr().td("Maroon", "5")
        );
    }
}

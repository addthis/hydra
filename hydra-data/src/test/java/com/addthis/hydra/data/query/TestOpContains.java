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


public class TestOpContains extends TestOp {

    @Test
    public void testMap() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("aab", "1").
                        tr().td("bab", "2").
                        tr().td("cba", "3"),
                "contains=0:ab",
                new DataTableHelper().
                        tr().td("aab", "1").
                        tr().td("bab", "2")
        );
    }
}

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

public class TestOpFill extends TestOp {

    @Test
    public void testNoop() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0"),
                "fill=0",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "6", "0")
        );
    }

    @Test
    public void testSimple() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a"),
                "fill=0",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a", "0", "0")
        );
    }

    @Test
    public void testPad() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2").
                        tr().td("dog", "a"),
                "pad=0:5",
                new DataTableHelper().
                        tr().td("cat", "a", "1", "2", "0").
                        tr().td("dog", "a", "0", "0", "0")
        );
    }


}

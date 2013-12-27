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

public class TestOpTranspose extends TestOp {

    @Test
    public void testTranspose() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1", "q").
                        tr().td("dog", "b", "2", "r").
                        tr().td("cow", "c", "3", "s"),
                "trans",
                new DataTableHelper().
                        tr().td("cat", "dog", "cow").
                        tr().td("a", "b", "c").
                        tr().td("1", "2", "3").
                        tr().td("q", "r", "s")
        );
    }

    @Test
    public void testTransposeWithNull() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("cat", "a", "1").
                        tr().td("dog", null, "2").
                        tr().td("cow", "c", "3"),
                "trans",
                new DataTableHelper().
                        tr().td("cat", "dog", "cow").
                        tr().td("a", null, "c").
                        tr().td("1", "2", "3")
        );
    }
}

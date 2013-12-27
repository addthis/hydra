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

public class TestOpTitle extends TestOp {

    @Test
    public void testTitle() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3"),
                "title",
                new DataTableHelper().
                        tr().td("0", "1", "2", "3").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3")
        );
    }

    @Test
    public void testTitleDeclared() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3"),
                "title=w,x,y,z",
                new DataTableHelper().
                        tr().td("w", "x", "y", "z").
                        tr().td("a", "1", "2", "3").
                        tr().td("b", "1", "2", "3").
                        tr().td("c", "1", "2", "3")
        );
    }
}

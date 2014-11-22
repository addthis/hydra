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

import com.addthis.hydra.data.query.op.OpLimit;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestOpLimit extends TestOp {

    @Test
    public void validInput() {
        assertTrue(OpLimit.validInput.matcher("1").matches());
        assertTrue(OpLimit.validInput.matcher("10").matches());
        assertTrue(OpLimit.validInput.matcher("10:10").matches());
        assertTrue(OpLimit.validInput.matcher("k10").matches());
        assertTrue(OpLimit.validInput.matcher("kk10").matches());
        assertTrue(OpLimit.validInput.matcher("kk10:10").matches());
        assertFalse(OpLimit.validInput.matcher("10k").matches());
        assertFalse(OpLimit.validInput.matcher("abcd").matches());
        assertFalse(OpLimit.validInput.matcher("").matches());
    }

    @Test
    public void testLimit() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2").
                        tr().td("b", "2", "3").
                        tr().td("c", "3", "4"),
                "limit=2",
                new DataTableHelper().
                        tr().td("a", "1", "2").
                        tr().td("b", "2", "3")
        );
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "1", "2").
                        tr().td("b", "2", "3").
                        tr().td("c", "3", "4"),
                "limit=1:1",
                new DataTableHelper().
                        tr().td("b", "2", "3")
        );
    }

    @Test
    public void testKeyColumnLimit() throws Exception {
        doOpTest(
                new DataTableHelper().
                                             tr().td("a", "1", "2").
                                             tr().td("a", "2", "4").
                                             tr().td("b", "5", "6").
                                             tr().td("b", "7", "8").
                                             tr().td("c", "1", "2").
                                             tr().td("c", "3", "4"),
                    "limit=k1",
                new DataTableHelper().
                                             tr().td("a", "1", "2").
                                             tr().td("b", "5", "6").
                                             tr().td("c", "1", "2")
        );
        doOpTest(
                new DataTableHelper().
                                             tr().td("a", "1", "2").
                                             tr().td("a", "2", "4").
                                             tr().td("b", "5", "6").
                                             tr().td("b", "7", "8").
                                             tr().td("c", "1", "2").
                                             tr().td("c", "3", "4"),
                "limit=k1:1",
                new DataTableHelper().
                                             tr().td("a", "2", "4").
                                             tr().td("b", "7", "8").
                                             tr().td("c", "3", "4")
        );
    }
}

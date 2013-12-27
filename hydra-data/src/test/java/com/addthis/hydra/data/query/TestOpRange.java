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

public class TestOpRange extends TestOp {

    @Test
    public void testRange() throws Exception {
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "range=1,4", parse("B 2 bot|C 3 cog|D 4 din"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "range=1,10", parse("B 2 bot|C 3 cog|D 4 din"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "range=3,2", parse("C 3 cog"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "range=2,3", parse("C 3 cog"));
        doOpTest(parse(""), "range=1,10", parse(""));
    }
}

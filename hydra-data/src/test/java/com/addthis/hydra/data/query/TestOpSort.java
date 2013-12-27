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

public class TestOpSort extends TestOp {

    @Test
    public void testSort() throws Exception {
        DataTableHelper basicTable = parse("A 1 art|B 2 bot|C 3 cog|D 4 din");
        DataTableHelper dataTable = parse("0 A 3|0 A 5|1 A 1|1 B 2");
        DataTableHelper dataTable2 = parse("3 A|2 B|4 C|1 X");
        DataTableHelper dataTableDecimals = parse("A .9 | B .8282 | C .95 | D .102392");
        doOpTest(parse(""), "sort=1,2,3:sns:d", parse(""));
        doOpTest(parse(""), "sort=1,2,3:sns:d", parse(""));
        doOpTest(basicTable, "sort=1:n:d", parse("D 4 din|C 3 cog|B 2 bot|A 1 art"));
        doOpTest(basicTable, "sort=0:x:d", parse("D 4 din|C 3 cog|B 2 bot|A 1 art")); // default is str compare
        doOpTest(dataTable, "sort=0,1,2:nsn:ada", parse("0 A 3|0 A 5|1 B 2|1 A 1"));
        doOpTest(dataTable, "sort=0,1,2:nsn:add", parse("0 A 5|0 A 3|1 B 2|1 A 1"));
        doOpTest(dataTable, "sort=1,2:sn:da", parse("1 B 2|1 A 1|0 A 3|0 A 5"));
        doOpTest(dataTable, "sort=0,1,2:ns:", parse("0 A 3|0 A 5|1 A 1|1 B 2"));
        doOpTest(dataTable2, "sort=0", parse("1 X|2 B|3 A|4 C"));
        doOpTest(dataTable2, "sort=0:", parse("1 X|2 B|3 A|4 C"));
        doOpTest(dataTable2, "sort=0::", parse("1 X|2 B|3 A|4 C"));
        doOpTest(dataTable2, "sort=", parse("1 X|2 B|3 A|4 C"));
        doOpTest(dataTable2, "sort", parse("1 X|2 B|3 A|4 C"));
        doOpTest(dataTableDecimals, "sort=1:d:a", parse("D .102392|B .8282|A .9|C .95"));
    }
}

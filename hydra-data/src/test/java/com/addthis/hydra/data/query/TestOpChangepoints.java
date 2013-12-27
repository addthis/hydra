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

public class TestOpChangepoints extends TestOp {

    @Test
    public void testSort() throws Exception {
        DataTableHelper basicTable = parse("MONDAY 10|TUESDAY 10|WEDNESDAY 10|THURSDAY 1000|FRIDAY 10|SATURDAY 10");
        doOpTest(basicTable, "changepoints=0:1", parse("THURSDAY PEAK 990"));
    }
}

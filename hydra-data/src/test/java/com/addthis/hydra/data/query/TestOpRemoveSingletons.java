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

public class TestOpRemoveSingletons extends TestOp {

    @Test
    public void testSort() throws Exception {
        DataTableHelper basicTable = parse("key1 val1 xxx|key1 val1 yyy|key1 val1 zzz|key2 val1 aaa|key2 val2 bbb|key2 val2 ccc");
        doOpTest(basicTable, "rmsing=0:1", parse("key2 val1 aaa|key2 val2 bbb|key2 val2 ccc"));
    }
}

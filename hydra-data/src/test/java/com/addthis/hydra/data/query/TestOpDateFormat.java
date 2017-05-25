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

import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

public class TestOpDateFormat extends TestOp {

    @Test
    public void testConvertDatesInPlace() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("140101").
                        tr().td("140108"),
                "datef=0:yyMMdd:yyww",
                new DataTableHelper().
                        tr().td("1401").
                        tr().td("1402")
        );
    }

    @Test
    public void testConvertDatesDifferentColumn() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("140101", "0").
                        tr().td("140108", "0"),
                "datef=0:yyMMdd:yyww:1",
                new DataTableHelper().
                        tr().td("140101", "1401").
                        tr().td("140108", "1402")
        );
    }

    @Test
    public void testParseUnixMillis() throws Exception {
        long testTimeMillis = 1418078000000l;
        String outputFormat = "yyMMdd";
        // Manually convert the test-time to yyMMdd in the local time zone
        String expectedResult = DateTimeFormat.forPattern(outputFormat).print(testTimeMillis);
        doOpTest(
                new DataTableHelper().
                        tr().td(Long.toString(testTimeMillis), "0"),
                String.format("datef=0:unixmillis:%s:1", outputFormat),
                new DataTableHelper().
                        tr().td(Long.toString(testTimeMillis), expectedResult)
        );
    }
}

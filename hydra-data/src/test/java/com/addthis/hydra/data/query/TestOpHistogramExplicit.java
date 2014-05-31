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

public class TestOpHistogramExplicit extends TestOp {

    @Test
    public void testHistogram() throws Exception {
        DataTableHelper s1 = new DataTableHelper().
                tr().td("A", "1", "art").
                tr().td("B", "2", "bot").
                tr().td("C", "3", "cog").
                tr().td("D", "4", "din").
                tr().td("D", "5", "erf");
        doOpTest(s1, "histo2=1,2,4", parse("-2147483648 1|2 2|4 2"));
        s1 = new DataTableHelper().
                tr().td("A", "1.0", "art").
                tr().td("A", "1.5", "art").
                tr().td("B", "2.0", "bot").
                tr().td("B", "2.5", "bot").
                tr().td("C", "3.0", "cog").
                tr().td("C", "3.5", "cog").
                tr().td("D", "4.0", "din").
                tr().td("D", "4.5", "din").
                tr().td("D", "5.0", "erf");
        doOpTest(s1, "histo2=1,2.0,4.0", parse("-Infinity 2|2.0 4|4.0 3"));
    }

}


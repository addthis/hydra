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

public class TestOpPercentileDistribution extends TestOp {

    @Test
    public void percentileDistribution() throws Exception {
        doOpTest(
                new DataTableHelper().
                        tr().td("a", "0").
                        tr().td("a", "1").
                        tr().td("b", "2").
                        tr().td("b", "3").
                        tr().td("b", "4"),
                "distribution=1,5",
                new DataTableHelper()
                        .tr().td(".5", "2.0")
                        .tr().td(".75", "3.5")
                        .tr().td(".95", "4.0")
                        .tr().td(".98", "4.0")
                        .tr().td(".99", "4.0")
                        .tr().td(".999","4.0")
        );
    }
}

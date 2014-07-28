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
package com.addthis.hydra.job.spawn;

import com.addthis.hydra.data.util.DateUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JobAlertUtilTest {

    @Test
    public void testExpandDateMacro() {
        String expanded = JobAlertUtil.expandDateMacro("{{now}}/{{now-1}},{{now}},123442/{{now, {{now}}");
        String now = DateUtil.getDateTime(JobAlertUtil.ymdFormatter, "{{now}}").toString(JobAlertUtil.ymdFormatter);
        String nowMinus1 = DateUtil.getDateTime(JobAlertUtil.ymdFormatter, "{{now-1}}").toString(JobAlertUtil.ymdFormatter);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

}

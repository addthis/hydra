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
package com.addthis.hydra.job.alert;

import com.addthis.hydra.data.util.DateUtil;
import com.addthis.hydra.job.alert.JobAlertUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JobAlertUtilTest {

    @Test
    public void expandDateMacro() {
        String expanded = JobAlertUtil.expandDateMacro("{{now}}/{{now-1}},{{now}},123442/{{now, {{now}}");
        String now = DateUtil.getDateTime(JobAlertUtil.ymdFormatter, "{{now}}").toString(JobAlertUtil.ymdFormatter);
        String nowMinus1 = DateUtil.getDateTime(JobAlertUtil.ymdFormatter, "{{now-1}}").toString(JobAlertUtil.ymdFormatter);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

    @Test
    public void expandDateMacroHourly() {
        String expanded = JobAlertUtil.expandDateMacro("{{now-0h}}/{{now-5h}},{{now+0h}},123442/{{now, {{now+0h}}");
        String now = DateUtil.getDateTime(JobAlertUtil.ymdhFormatter, "{{now}}", true).toString(JobAlertUtil.ymdhFormatter);
        String nowMinus1 = DateUtil.getDateTime(JobAlertUtil.ymdhFormatter, "{{now-5}}", true).toString(JobAlertUtil.ymdhFormatter);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

    @Test
    public void expandDateMacroHourlyWithTimeZone() {
        String expanded = JobAlertUtil.expandDateMacro("{{now-0h:EST}}/{{now-5h:EST}},{{now+0h:EST}},123442h/{{now, {{now+0h:EST}}");
        String now = DateUtil.getDateTime(JobAlertUtil.ymdhFormatter, "{{now:EST}}", true).toString(JobAlertUtil.ymdhFormatter);
        String nowMinus1 = DateUtil.getDateTime(JobAlertUtil.ymdhFormatter, "{{now-5:EST}}", true).toString(JobAlertUtil.ymdhFormatter);
        assertEquals(String.format("%s/%s,%s,123442h/{{now, %s",now,nowMinus1,now,now), expanded);
    }

}

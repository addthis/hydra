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
package com.addthis.hydra.data.util;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDateUtil {

    @Test
    public void expandDateMacro() {
        String expanded = DateUtil.expandDateMacro("{{now}}/{{now-1}},{{now}},123442/{{now, {{now}}");
        String now = DateUtil.getDateTime(DateUtil.ymdFormatter, "{{now}}").toString(DateUtil.ymdFormatter);
        String nowMinus1 = DateUtil.getDateTime(DateUtil.ymdFormatter, "{{now-1}}").toString(DateUtil.ymdFormatter);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

    @Test
    public void withFormatter() {
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyyww");
        String expanded = DateUtil.expandDateMacro("{{now::yyyyww}}/{{now-1::yyyyww}},{{now::yyyyww}},123442/{{now, {{now::yyyyww}}");
        String now = DateUtil.getDateTime(format, "{{now}}").toString(format);
        String nowMinus1 = DateUtil.getDateTime(format, "{{now-1}}").toString(format);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

    @Test
    public void expandDateMacroHourly() {
        String expanded = DateUtil.expandDateMacro("{{now-0h}}/{{now-5h}},{{now+0h}},123442/{{now, {{now+0h}}");
        String now = DateUtil.getDateTime(DateUtil.ymdhFormatter, "{{now}}", true).toString(DateUtil.ymdhFormatter);
        String nowMinus1 = DateUtil.getDateTime(DateUtil.ymdhFormatter, "{{now-5}}", true).toString(DateUtil.ymdhFormatter);
        assertEquals(String.format("%s/%s,%s,123442/{{now, %s",now,nowMinus1,now,now), expanded);
    }

    @Test
    public void expandDateMacroHourlyWithTimeZone() {
        String expanded = DateUtil.expandDateMacro("{{now-0h:EST}}/{{now-5h:EST}},{{now+0h:EST}},123442h/{{now, {{now+0h:EST}}");
        String now = DateUtil.getDateTime(DateUtil.ymdhFormatter, "{{now:EST}}", true).toString(DateUtil.ymdhFormatter);
        String nowMinus1 = DateUtil.getDateTime(DateUtil.ymdhFormatter, "{{now-5:EST}}", true).toString(DateUtil.ymdhFormatter);
        assertEquals(String.format("%s/%s,%s,123442h/{{now, %s",now,nowMinus1,now,now), expanded);
    }


}

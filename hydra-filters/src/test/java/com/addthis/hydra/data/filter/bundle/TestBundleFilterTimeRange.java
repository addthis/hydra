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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.config.Configs;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class TestBundleFilterTimeRange {
    static final private String BEFORE_160923 = "time:TIME, before:2016-09-23, timeFormat:YYYY-MM-dd";
    static final private String BEFORE_150925 = "time:TIME, before:2015-09-25, timeFormat:YYYY-MM-dd";
    static final private String AFTER_160921  = "time:TIME,  after:2016-09-21, timeFormat:YYYY-MM-dd";
    static final private String AFTER_170921  = "time:TIME,  after:2017-09-21, timeFormat:YYYY-MM-dd";
    static private Bundle bundle;

    @Before
    public void before() throws IOException {
        bundle = Configs.decodeObject(Bundle.class, "TIME:1474545600000");   // 09/22/2016
    }

    @Test
    public void testBeforeNoTimezone() throws IOException {
        BundleFilterTimeRange filter1 = Configs.decodeObject(BundleFilterTimeRange.class, BEFORE_160923);
        BundleFilterTimeRange filter2 = Configs.decodeObject(BundleFilterTimeRange.class, BEFORE_150925);
        assertTrue("Expected true since filtered out later than 160923", filter1.filter(bundle));
        assertFalse("Expected false since filtered out later than 150925", filter2.filter(bundle));
    }

    @Test
    public void testAfterNoTimezone() throws IOException {
        BundleFilterTimeRange filter1 = Configs.decodeObject(BundleFilterTimeRange.class, AFTER_160921);
        BundleFilterTimeRange filter2 = Configs.decodeObject(BundleFilterTimeRange.class, AFTER_170921);
        assertTrue("Expected true since filtered out earlier than 160921", filter1.filter(bundle));
        assertFalse("Expected false since filtered out earlier than 170921", filter2.filter(bundle));
    }

    @Test
    public void testMinusBefore() throws IOException {
        BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, "time:TIME, before:-20160923, timeFormat:YYYYMMdd");
        assertFalse(filter.filter(bundle));
    }

    @Test
    public void testBeforeAfterInRange() throws IOException {
        String str = "time:TIME, before:20170101, after:20120101, timeFormat:YYYYMMDD";
        BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
        filter.filter(bundle);
        assertTrue( filter.filter(bundle));
    }

    @Test
    public void testBeforeAfterOutRange() throws IOException {
        String str = "time:TIME, before:1212011213, after:1301011415, defaultExit:true, " +
                "timeFormat:yyMMddHHmm, timeZone=Australia/Brisbane";
        BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
        assertFalse(filter.filter(bundle));
    }

    @Test
    public void testNoTime() throws IOException {
        String str = "time:null, before:20170101, after:20120101,timeFormat:YYYYMMDD";
        try {
            BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
            fail("Expected exception: time field is required");
        } catch(JsonMappingException jme) {
            assertThat(jme.getMessage(), containsString("marked as required"));
        }
    }

    @Test
    public void testMinusBeforeBadTimeformat() throws IOException {
        String str = "time:TIME, before:-2016/09/23, timeFormat:YYYY/MM/dd";
        try {
            BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
            fail("Expected exception: wrong format for date input");
        } catch(JsonMappingException jme) {
            assertThat(jme.getMessage(), containsString("value failed: For input string"));
        }
    }

    @Test
    public void testNoTimeformat() throws IOException {
        String str = "time:TIME, before:20170101, after:20120101, defaultExit:true, timeFormat:null, timeZone:EST";
        try {
            BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
            fail("Expected exception: no timeformat");
        } catch(JsonMappingException jme) {
            assertThat(jme.getMessage(), containsString("Cannot parse"));
        }
    }

    @Test
    public void testNoTimeformatNoTimezone() throws IOException {
        String str = "time:TIME, before:20170101, after:20120101, defaultExit:true, timeFormat:null, timeZone:null";
        try {
            BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class, str);
            fail("Expected exception: no timeformat");
        } catch(JsonMappingException jme) {
            assertThat(jme.getMessage(), containsString("Cannot parse"));
        }
    }
}

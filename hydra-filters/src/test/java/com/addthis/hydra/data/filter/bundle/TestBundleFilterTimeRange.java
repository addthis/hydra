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
import com.addthis.bundle.util.AutoField;
import com.addthis.codec.config.Configs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class TestBundleFilterTimeRange {
    AutoField timeIn ;
    DateTime utc;
    DecimalFormat df;
    Bundle bundle;

    static final String before1 = "time:TIME, before:2016-09-23, timeFormat:YYYY-MM-dd";
    static final String before2 = "time:TIME, before:2015-09-25, timeFormat:YYYY-MM-dd";
    static final String after1  = "time:TIME,  after:2016-09-21, timeFormat:YYYY-MM-dd";
    static final String after2  = "time:TIME,  after:2017-09-21, timeFormat:YYYY-MM-dd";

    @Before
    public void before() throws IOException {
        this.timeIn = mock(AutoField.class);
        utc = new DateTime(DateTimeZone.UTC);
        df = new DecimalFormat("00");
        bundle = Configs.decodeObject(Bundle.class, "TIME:1474502400000");   // 09/22/2016
    }

    @Test
    public void testBeforeNoTimeZone() throws IOException {
        BundleFilterTimeRange filter1 = Configs.decodeObject(BundleFilterTimeRange.class, before1);
        BundleFilterTimeRange filter2 = Configs.decodeObject(BundleFilterTimeRange.class, before2);
        assertTrue(filter1.filter(bundle));
        assertFalse(filter2.filter(bundle));
    }

    @Test
    public void testAfterNoTimeZone() throws IOException {
        BundleFilterTimeRange filter1 = Configs.decodeObject(BundleFilterTimeRange.class, after1);
        BundleFilterTimeRange filter2 = Configs.decodeObject(BundleFilterTimeRange.class, after2);
        assertTrue(filter1.filter(bundle));
        assertFalse(filter2.filter(bundle));
    }

    @Test(expected= Exception.class)
    public void testAfterNoTimeFormat() throws IOException {
        BundleFilterTimeRange filter = Configs.decodeObject(BundleFilterTimeRange.class,
                "time:TIME,before:2016-09-30,timeFormat:null,timeZone:EST");
    }

    @Test
    public void testTimeZoneAB() {
        DateTimeZone tz = DateTimeZone.forID("Australia/Brisbane");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "1212011213", "1301011415", true, "yyMMddHHmm", "Australia/Brisbane");
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testTimeZoneNY() {
        DateTimeZone tz = DateTimeZone.forID("America/New_York");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "1212011314", "1301011516", true, "yyMMddHHmm", "America/New_York");
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testTimeZoneLA() {
        DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "1212011314", "1301011516", true, "yyMMddHHmm", "America/Los_Angeles");
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testNoTimeZone() {
        DateTimeZone tz = DateTimeZone.forID("EST");
        DateTime ntzDateTime = utc.toDateTime(tz);
        String input = ntzDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "1609220820", "1301011415", true, "yyMMddHHmm", null);
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testNoTimeFormat() {
        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "121201", "130101", true, null, "EST");
        DateTimeFormatter format = bftr.getTimeZoneFormat();
        assertNull(format);
    }

    @Test
    public void testZoneNoTimeFormat() throws IOException {
        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "121201", "130101", true, null, null);
        DateTimeFormatter format = bftr.getTimeZoneFormat();
        assertNull(format);
    }
}

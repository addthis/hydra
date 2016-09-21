package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.util.AutoField;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Created by irene.cho on 9/19/16.
 */
public class TestBundleFilterTimeRange {
    AutoField timeIn ;
    DateTime utc;
    DecimalFormat df;

    @Before
    public void before () {
        this.timeIn = mock(AutoField.class);
        utc = new DateTime(DateTimeZone.UTC);
        df = new DecimalFormat("00");
    }

    @Test
    public void testBundleFilterTimeRangeAB() {
        DateTimeZone tz = DateTimeZone.forID("Australia/Brisbane");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "121201", "130101", true, "yyMMddHHmm", "Australia/Brisbane");
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testBundleFilterTimeRangeEST() {
        DateTimeZone tz = DateTimeZone.forID("America/New_York");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "121201", "130101", true, "yyMMddHHmm", "America/New_York");

        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testBundleFilterTimeRangePST() {
        DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");
        DateTime abDateTime = utc.toDateTime(tz);
        String input = abDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange(timeIn, "121201", "130101", true, "yyMMddHHmm", "America/Los_Angeles");

        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testBundleFilterTimeRangeNoTimeZone() {
        DateTimeZone tz = DateTimeZone.forID("EST");
        DateTime ntzDateTime = utc.toDateTime(tz);
        String input = ntzDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "121201", "130101", true, "yyMMddHHmm", null);
        DateTimeFormatter format = bftr.getTimeZoneFormat();

        String time = format.parseDateTime(input).getYear() + "" +
                df.format(format.parseDateTime(input).getMonthOfYear()) + "" +
                df.format(format.parseDateTime(input).getDayOfMonth()) + "" +
                df.format(format.parseDateTime(input).getHourOfDay()) + "" +
                df.format(format.parseDateTime(input).getMinuteOfHour());
        assertEquals(input, time.substring(2));
    }

    @Test
    public void testBundleFilterTimeRangeNoTimeFormat() {
        DateTimeZone tz = DateTimeZone.forID("EST");
        DateTime ntzntfDateTime = utc.toDateTime(tz);
        String input = ntzntfDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "121201", "130101", true, null, "EST");
        DateTimeFormatter format = bftr.getTimeZoneFormat();
        assertNull(format);
        assertEquals("EST", bftr.getTimeZone());
    }

    @Test
    public void testBundleFilterTimeRangeNoTimeZoneNoTimeFormat() {
        DateTimeZone tz = DateTimeZone.forID("EST");
        DateTime ntzntfDateTime = utc.toDateTime(tz);
        String input = ntzntfDateTime.toString("yyMMddHHmm");

        BundleFilterTimeRange bftr = new BundleFilterTimeRange( timeIn, "121201", "130101", true, null, null);
        DateTimeFormatter format = bftr.getTimeZoneFormat();
        assertNull(format);
    }
}

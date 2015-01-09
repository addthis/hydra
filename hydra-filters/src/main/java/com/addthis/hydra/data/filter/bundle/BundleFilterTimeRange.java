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
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueLong;
import com.addthis.codec.annotations.FieldConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">provides a time range inclusion or exclusion filter</span>.
 * <p/>
 * <p>The 'before' and 'after' fields can be used to filter the input date and time.
 * For the 'before' and 'after' fields, a six character string is interpreted by default as "yyMMdd",
 * an eight character string is interpreted by default as "yyMMddHH", and
 * a ten character string is interpreted by default is "yyMMddHHmm". A string
 * of the form "-NNN" is interpreted as NNN days in the past.
 * </p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    // Accept all date/time values in between December 1, 2012 and January 1, 2013.
 *    {op:"time-range", time: "TIME", after: "121201", before: "130101"},
 * </pre>
 *
 * @user-reference
 * @hydra-name time-range
 */
public class BundleFilterTimeRange extends BundleFilter {

    private static final DateTimeFormatter ymd   = DateTimeFormat.forPattern("yyMMdd");
    private static final DateTimeFormatter ymdh  = DateTimeFormat.forPattern("yyMMddHH");
    private static final DateTimeFormatter ymdhm = DateTimeFormat.forPattern("yyMMddHHmm");

    /**
     * The field containing date/time input as expressed in UNIX milliseconds. This field is required.
     */
    final private AutoField time;

    /**
     * If non-null then filter out all date/time values that occur later than this value.
     */
    final private String before;

    /**
     * If non-null then filter out all date/time values that occur earlier than this value.
     */
    final private String after;

    /**
     * The value to return when a date/time value is within the filter(s). Default is true.
     */
    final private boolean defaultExit;

    /**
     * If non-null then parse the 'before' and 'after' fields using this
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat
     * .html">DateTimeFormat</a>.
     */
    final private String timeFormat;

    final private long              tbefore;
    final private long              tafter;
    final private DateTimeFormatter format;

    @JsonCreator
    public BundleFilterTimeRange(@JsonProperty(value = "time", required = true) AutoField time,
                                 @JsonProperty("before") String before,
                                 @JsonProperty("after") String after,
                                 @JsonProperty("defaultExit") boolean defaultExit,
                                 @JsonProperty("timeFormat") String timeFormat) {
        this.time = time;
        this.before = before;
        this.after = after;
        this.defaultExit = defaultExit;
        this.timeFormat = timeFormat;

        format = (timeFormat != null) ? DateTimeFormat.forPattern(timeFormat) : null;
        tbefore = (before != null) ? convertDate(before) : null;
        tafter = (after != null) ? convertDate(after) : null;
    }

    @Override
    public void open() { }

    @Override
    public boolean filter(Bundle bundle) {
        ValueLong timeValue = time.getValue(bundle).asLong();
        if (timeValue == null) {
            return defaultExit;
        }
        long unixTime = timeValue.getLong();
        if (before != null && unixTime > tbefore) {
            return false;
        }
        if (after != null && unixTime < tafter) {
            return false;
        }
        return defaultExit;
    }

    private long convertDate(String date) {
        // return days offset into the past
        if (date.startsWith("-")) {
            return new DateTime().minusDays(Integer.parseInt(date.substring(1))).toDate().getTime();
        }
        if (format != null) {
            return format.parseMillis(date);
        }
        // return YYMMDD(HH)(MM) formatted time
        switch (date.length()) {
            case 6:
                return ymd.parseMillis(date);
            case 8:
                return ymdh.parseMillis(date);
            case 10:
                return ymdhm.parseMillis(date);
            default:
                return 0;
        }
    }

}

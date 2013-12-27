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
import com.addthis.bundle.value.ValueLong;
import com.addthis.codec.Codec;

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

    private static DateTimeFormatter ymd = DateTimeFormat.forPattern("yyMMdd");
    private static DateTimeFormatter ymdh = DateTimeFormat.forPattern("yyMMddHH");
    private static DateTimeFormatter ymdhm = DateTimeFormat.forPattern("yyMMddHHmm");

    /**
     * The date/time input as expressed in UNIX milliseconds.
     */
    @Codec.Set(codable = true, required = true)
    private String time;

    /**
     * If non-null then filter out all date/time values that occur later than this value.
     */
    @Codec.Set(codable = true)
    private String before;

    /**
     * If non-null then filter out all date/time values that occur earlier than this value.
     */
    @Codec.Set(codable = true)
    private String after;

    /**
     * The value to return when a date/time value is within the filter(s). Default is true.
     */
    @Codec.Set(codable = true)
    private boolean defaultExit = true;

    /**
     * If non-null then parse the 'before' and 'after' fields using this
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     */
    @Codec.Set(codable = true)
    private String timeFormat;

    private long tbefore;
    private long tafter;
    private DateTimeFormatter format;
    private String fields[];

    @Override
    public void initialize() {
        fields = new String[]{time};
        if (timeFormat != null) {
            format = DateTimeFormat.forPattern(timeFormat);
        }
        if (before != null) {
            tbefore = convertDate(before);
        }
        if (after != null) {
            tafter = convertDate(after);
        }
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField bound[] = getBindings(bundle, fields);
        ValueLong timeValue = bundle.getValue(bound[0]).asLong();
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

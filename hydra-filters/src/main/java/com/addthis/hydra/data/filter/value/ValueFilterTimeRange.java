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
package com.addthis.hydra.data.filter.value;

import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns one or more formatted time values</span>.
 * <p/>
 * <p>The input to this filter is assumed to be a date in Unix milliseconds.
 * This behavior can be changed using the {@link #now now} or {@link #defaultNow defaultNow} fields.
 * The output is an array of length {@link #rangeDays rangeDays} or {@link #rangeHours rangeHours}.
 * For example if rangeDays is 10 then an array of length 10 is returned containing a formatted time
 * string for the input day and each of the next nine days. To begin counting at 1 instead of 0,
 * using the {@link #offsetHours offsetHours} or {@link #offsetDays offsetDays} field.
 * <p/>
 * <p>If the rangeDays and rangeHours parameters are not used then this filter
 * returns a single string value. If rangeDays has the value N for N > 0, then
 * this filter returns an array of N elements. Similarly an array is returned
 * if rangeHours is used.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    // return yesterday's date as a string
 *    {op: "time-range", offsetDays:-1, now:true, format:"YYMMdd"}
 *
 *    // return an array of yesterday's, today's, and tomorrow's dates
 *    {op: "time-range", offsetDays:-1, rangeDays:3, now:true, format:"YYMMdd"}
 *
 *    // return an array containing the input date and the next two days
 *    {op: "time-range", rangeDays:3, format:"YYMMdd"}
 * </pre>
 *
 * @user-reference
 * @hydra-name time-range
 */
public class ValueFilterTimeRange extends ValueFilter {

    /**
     * The output format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     * This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private String format;

    /**
     * Offset the input time by this number of days. Default is 0.
     */
    @Codec.Set(codable = true)
    private int offsetDays;

    /**
     * Offset the input time by this number of hours. Default is 0.
     */
    @Codec.Set(codable = true)
    private int offsetHours;

    /**
     * The number of days to return. Cannot be used together in conjunction with rangeHours.
     */
    @Codec.Set(codable = true)
    private int rangeDays;

    /**
     * The number of hours to return. Cannot be used together in conjunction with rangeDays.
     */
    @Codec.Set(codable = true)
    private int rangeHours;

    /**
     * If true, use the current time when the input is 0. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean defaultNow;

    /**
     * If true, then ignore the input value and use the current time. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean now;

    private DateTimeFormatter date;

    private final void init() {
        if (date == null) {
            date = DateTimeFormat.forPattern(format);
            if (rangeHours > 0 && rangeDays > 0) {
                throw new RuntimeException("rangeHours and rangeDays cannot both be > 0");
            }
        }
    }

    private ValueArray getTimeRange(long time, int range, TimeUnit timeUnit) {
        ValueArray arr = ValueFactory.createArray(range);
        for (int i = 0; i < range; i++) {
            arr.append(ValueFactory.create(date.print(time + timeUnit.toMillis(i))));
        }
        return arr;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        init();
        try {
            long time = now ? JitterClock.globalTime() : ValueUtil.asNumberOrParseLong(value, 10).asLong().getLong();
            if (time == 0 && defaultNow) {
                time = JitterClock.globalTime();
            }
            if (offsetDays != 0) {
                time += TimeUnit.DAYS.toMillis(offsetDays);
            }
            if (offsetHours != 0) {
                time += TimeUnit.HOURS.toMillis(offsetHours);
            }
            if (rangeHours > 0) {
                return getTimeRange(time, rangeHours, TimeUnit.HOURS);
            }
            if (rangeDays > 0) {
                return getTimeRange(time, rangeDays, TimeUnit.DAYS);
            }
            return ValueFactory.create(date.print(time));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

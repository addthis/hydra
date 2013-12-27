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

import java.util.SortedSet;
import java.util.TreeSet;

import com.addthis.basis.time.DTimeUnit;
import com.addthis.basis.time.Dates;
import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">calculates the total numbers of days represented
 * in the input string</span>.
 * <p/>
 * <p>The input string is a sequence of items, where an item is either
 * a single day or a set of two days. The items are separated by {@link #dateRangeSep dateRangeSep}.
 * A set of two days is separated by {@link #dateSep dateSep}. The dates must be formatted
 * according to {@link #dateFormat dateFormat}. Each item in the sequence that is a single day
 * increments the output counter by one. Each item in the sequence that is a set of two days
 * increments the output counter by the number of days in between the two dates.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"field", from:"NUM_DAYS", filter:{op:"data-range-length",dateFormat:"ddMMyy"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name data-range-length
 */
public class ValueFilterDateRangeLength extends StringFilter {

    /**
     * The input format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     * Default is "yyMMdd".
     */
    @Codec.Set(codable = true)
    private String dateFormat = "yyMMdd";

    /**
     * The separator in between (start date, end date) pairs. Default is "-" .
     */
    @Codec.Set(codable = true)
    private String dateSep = "-";

    /**
     * The separator in between items in the input sequence. Default is "," .
     */
    @Codec.Set(codable = true)
    private String dateRangeSep = ",";

    /**
     * This field is not used. Do whatever you want with it.
     */
    @Codec.Set(codable = true)
    private String diffUnit = "days";

    public ValueFilterDateRangeLength() {
    }

    public ValueFilterDateRangeLength(String dateFormat, String dateSep, String diffUnit) {
        this.dateFormat = dateFormat;
        this.dateSep = dateSep;
        this.diffUnit = diffUnit;
    }

    @Override
    public String filter(String value) {
        if (value != null) {
            try {
                return Long.toString(countDays(value));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return value;
    }

    protected Interval parseRange(String rangeString) {
        int sepIndex = rangeString.indexOf(dateSep);
        if (sepIndex <= 0 || sepIndex + 1 == rangeString.length()) {
            throw new IllegalArgumentException("Failed to parse date range: " + rangeString);
        }

        DateTimeFormatter dtf = DateTimeFormat.forPattern(dateFormat);
        DateTime beg = dtf.parseDateTime(rangeString.substring(0, sepIndex));
        DateTime end = dtf.parseDateTime(rangeString.substring(sepIndex + 1, rangeString.length()));

        return new Interval(beg, end);
    }

    private String[] toArray(String dates) {
        return Strings.splitArray(dates, (dateRangeSep));
    }

    protected int countDays(String dates) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(dateFormat);
        String[] datesSplit = toArray(dates);
        SortedSet<DateTime> dateSet = new TreeSet<DateTime>();
        for (String strVal : datesSplit) {
            if (strVal.indexOf(dateSep) < 0) {
                dateSet.add(dtf.parseDateTime(strVal));
            } else {
                Interval interval = parseRange(strVal);
                for (DateTime dt : Dates.iterableInterval(interval, DTimeUnit.DAY)) {
                    dateSet.add(dt);
                }
            }
        }
        return dateSet.size();
    }
}

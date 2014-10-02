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

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @user-reference
 */
public final class TimeField implements SuperCodable {

    private final Logger log = LoggerFactory.getLogger(TimeField.class);

    public TimeField setField(String field) {
        this.field = field;
        return this;
    }

    public TimeField setFormat(String format) {
        this.format = format;
        return this;
    }

    public TimeField setTimeZone(String zone) {
        this.timeZone = zone;
        return this;
    }

    public String getField() {
        return field;
    }

    public String getFormat() {
        return format;
    }

    public String getTimeZone() {
        return timeZone;
    }

    /**
     * Field within the bundle to interpret as a time value.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String field;

    /**
     * Format for a time value. Specify "native"
     * for unix milliseconds in base 10 format.
     * Specify "unixmillis:NN" for unix milliseconds
     * in base NN format. Otherwise interpret the
     * value as a
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat
     * .html">DateTimeFormat</a>.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String format;

    /**
     * Optionally specify the timezone. Default is null.
     */
    @FieldConfig(codable = true)
    private String timeZone;

    private int               radix;
    private DateTimeFormatter formatter;

    public long toUnix(ValueObject val) {
        if (radix > 0) {
            Numeric num = ValueUtil.asNumberOrParseLong(val, radix);
            return num != null ? num.asLong().getLong() : JitterClock.globalTime();
        } else {
            try {
                return formatter.parseDateTime(ValueUtil.asNativeString(val)).getMillis();
            } catch (IllegalArgumentException e) {
                log.warn("unable to parse date time for val starting with : " +
                         Strings.printable(Strings.trunc(ValueUtil.asNativeString(val), 256)));
                return -1;
            }
        }
    }

    public ValueObject toValue(long unix) {
        if (radix > 0) {
            if (radix == 10) {
                return ValueFactory.create(unix);
            }
            return ValueFactory.create(Long.toString(unix, radix));
        } else {
            return ValueFactory.create(formatter.print(unix));
        }
    }

    @Override
    public void postDecode() {
        if (format.equals("native")) {
            format = "unixmillis:10";
        }
        if (format.startsWith("unixmillis")) {
            if (format.indexOf(":") > 0) {
                radix = Integer.parseInt(format.substring(format.indexOf(":") + 1));
            } else {
                radix = 10;
            }
        } else {
            formatter = DateTimeFormat.forPattern(format);
            if (timeZone != null) {
                formatter = formatter.withZone(DateTimeZone.forID(timeZone));
            }
        }
    }

    @Override
    public void preEncode() {
    }
}

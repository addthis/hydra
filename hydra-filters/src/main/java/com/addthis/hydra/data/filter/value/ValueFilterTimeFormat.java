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


import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.util.TimeField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">accepts a time string and converts the time into another format</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"DATE_EAST_SIDE", to:"DATE_WEST_SIDE", filter:{op:"time-format", timeZoneIn:"EST", timeZoneOut:"PST"}}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterTimeFormat extends AbstractValueFilter {

    /**
     * The input format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat
     * .html">DateTimeFormat</a>.
     * Default is "native".
     */
    private final String formatIn;

    /**
     * The input time zone. This field must be specified.
     */
    private final String timeZoneIn;

    /**
     * The output format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat
     * .html">DateTimeFormat</a>.
     */
    private final String formatOut;

    /**
     * The output time zone.
     */
    private final String timeZoneOut;

    private final TimeField in;
    private final TimeField out;


    @JsonCreator
    public ValueFilterTimeFormat(@JsonProperty("formatIn") String formatIn,
                                 @JsonProperty("timeZoneIn") String timeZoneIn,
                                 @JsonProperty(value = "formatOut", required = true) String formatOut,
                                 @JsonProperty("timeZoneOut") String timeZoneOut) {
        this.formatIn = formatIn;
        this.timeZoneIn = timeZoneIn;
        this.formatOut = formatOut;
        this.timeZoneOut = timeZoneOut;
        this.in = new TimeField(null, formatIn, timeZoneIn);
        this.out = new TimeField(null, formatOut, timeZoneOut);
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        long unixTime = in.toUnix(value);
        if (unixTime >= 0) {
            return out.toValue(unixTime);
        } else {
            return null;
        }
    }

}

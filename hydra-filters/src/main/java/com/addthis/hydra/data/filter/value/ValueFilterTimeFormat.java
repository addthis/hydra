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
import com.addthis.codec.Codec;
import com.addthis.hydra.data.util.TimeField;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">accepts a time string and converts the time into another format</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"DATE_EAST_SIDE", to:"DATE_WEST_SIDE", filter:{op:"time-format", timeZoneIn:"EST", timeZoneOut:"PST"}}
 * </pre>
 *
 * @user-reference
 * @hydra-name time-format
 */
public class ValueFilterTimeFormat extends ValueFilter implements Codec.SuperCodable {

    /**
     * The input format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     * Default is "native".
     */
    @Codec.Set(codable = true)
    private String formatIn = "native";

    /**
     * The input time zone. This field must be specified.
     */
    @Codec.Set(codable = true)
    private String timeZoneIn;

    /**
     * The output format using the
     * <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a>.
     */
    @Codec.Set(codable = true, required = true)
    private String formatOut;

    /**
     * The output time zone.
     */
    @Codec.Set(codable = true)
    private String timeZoneOut;

    private TimeField in;
    private TimeField out;

    @Override
    public ValueObject filterValue(ValueObject value) {
        long unixTime = in.toUnix(value);
        if (unixTime >= 0) {
            return out.toValue(unixTime);
        } else {
            return null;
        }
    }

    @Override
    public void postDecode() {
        in = new TimeField().setFormat(formatIn).setTimeZone(timeZoneIn);
        out = new TimeField().setFormat(formatOut).setTimeZone(timeZoneOut);
        in.postDecode();
        out.postDecode();
    }

    @Override
    public void preEncode() {
    }
}

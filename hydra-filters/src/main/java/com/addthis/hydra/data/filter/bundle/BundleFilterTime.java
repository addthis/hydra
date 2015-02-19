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

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.util.TimeField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">converts various time formats</span>.
 * <p/>
 * <p>If {@link #src src} is used then the date and time
 * from the src field is copied into the {@link #dst dst} field.
 * If the src field is not used then the current time is copied
 * into the dst field. The filter returns false if the src field is used but cannot be
 * interpreted as a legal time value. Otherwise
 * the filter returns true.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name time
 */
public class BundleFilterTime implements BundleFilter {

    private final Logger log = LoggerFactory.getLogger(BundleFilterTime.class);

    /**
     * Date and time to convert.
     */
    @FieldConfig(codable = true)
    private TimeField src;

    /**
     * Output time format.
     */
    @FieldConfig(codable = true)
    private TimeField dst;

    public BundleFilterTime setInput(TimeField src) {
        this.src = src;
        return this;
    }

    public BundleFilterTime setOutput(TimeField dst) {
        this.dst = dst;
        return this;
    }

    public TimeField getInput() {
        return src;
    }

    public TimeField getOutput() {
        return dst;
    }

    @Override
    public boolean filter(Bundle row) {
        if (dst != null) {
            long unixTime;
            if (src != null) {
                ValueObject in = src.getField().getValue(row);
                if (in == null) {
                    return false;
                }
                try {
                    unixTime = src.toUnix(in);
                } catch (NumberFormatException nfe) {
                    log.warn("Unable to parse time input field to long, input field was: " + in.toString());
                    return false;
                }
            } else {
                unixTime = JitterClock.globalTime();
            }
            dst.getField().setValue(row, dst.toValue(unixTime));
        }
        return true;
    }
}

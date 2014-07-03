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

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">converts a number from one base to another</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name intbase
 */
public class ValueFilterIntBase extends ValueFilter {

    private static final Logger log = LoggerFactory.getLogger(ValueFilterIntBase.class);

    /**
     * The radix of the input value. Must be specified if <code>inDouble</code> is false.
     */
    @FieldConfig(codable = true)
    private int in;

    /**
     * The radix of the output value. Must be specified if <code>outAsLong</code> is false.
     */
    @FieldConfig(codable = true)
    private int out;

    /**
     * If true, then return the output as a long. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean outAsLong = false;

    /**
     * If true, then the input is parsed as a double value. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean inDouble = false;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return value;
        }
        try {
            Numeric<?> num;
            if (!inDouble) {
                num = ValueUtil.asNumberOrParseLong(value, in);
            } else {
                num = ValueUtil.asNumberOrParseDouble(value);
            }

            if (outAsLong) {
                return num.asLong();
            } else {
                return ValueFactory.create(Long.toString(num.asLong().getLong(), out));
            }
        } catch (Exception ex)  {
            log.warn("", ex);
            return value;
        }
    }

}

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

import com.addthis.basis.util.NumberUtils;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * Convert between bases with custom alphabet choices. Here "decode" means
 * return a 10-base String representation.
 */
public class ValueFilterBaseConv extends ValueFilter {

    @FieldConfig(codable = true)
    private boolean decode;
    @FieldConfig(codable = true)
    private String  baseType;
    @FieldConfig(codable = true)
    private String  defaultValue;

    public ValueFilterBaseConv() {
    }

    public ValueFilterBaseConv(boolean decode, String baseType) {
        this.decode = decode;
        this.baseType = baseType;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (decode) {
            return decode(value);
        }
        return null;
    }

    protected ValueObject decode(ValueObject value) {
        try {
            Long decodedVal = null;
            if (baseType.equals("theo36")) {
                decodedVal = NumberUtils.longFromBase36(value.toString());
            } else if (baseType.equals("theo64")) {
                decodedVal = NumberUtils.longFromBase64(value.toString());
            }
            // base 10 fall-through
            else {
                switch (value.getObjectType()) {
                    case STRING:
                        decodedVal = new Long(value.asString().toString());
                        break;
                    default:
                        return value;
                }
            }
            return ValueFactory.create(decodedVal);
        } catch (RuntimeException ex) {
            if (defaultValue != null) {
                return ValueFactory.create(defaultValue);
            }
            throw ex;
        }
    }
}

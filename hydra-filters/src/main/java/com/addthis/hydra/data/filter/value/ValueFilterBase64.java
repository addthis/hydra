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

import com.addthis.basis.util.Base64;
import com.addthis.basis.util.LessStrings;

import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">encodes or decodes a value to
 * <a href="http://en.wikipedia.org/wiki/Base64">base64</a> format</span>.
 * <p/>
 * <p>Enabling <a href="#encode">encode</a> will encode the value to base64 format.
 * Enabling <a href="#decode">decode</a> will decode the from base64 format. If both
 * encode and decode are <code>false</code> then this filter
 * performs no operation. If both
 * encode and decode are <code>true</code> then this filter
 * will first encode the value and then decode the encoding, which is not recommended.
 * <p>Example:</p>
 * <pre>
 *   {from:"FOO", to:"BAR", base64.decode:true}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterBase64 extends StringFilter {

    /**
     * if <code>true</code> then encode the value
     */
    @FieldConfig(codable = true)
    private boolean encode;

    /**
     * if <code>true</code> then decode the value
     */
    @FieldConfig(codable = true)
    private boolean decode;

    @Override
    public String filter(String value) {
        if (!LessStrings.isEmpty(value)) {
            if (encode) {
                value = Base64.encode(value);
            }
            if (decode) {
                try {
                    value = Base64.decode(value, false);
                } catch (Exception ex) {
                    return null;
                }
            }
        }
        return value;
    }
}

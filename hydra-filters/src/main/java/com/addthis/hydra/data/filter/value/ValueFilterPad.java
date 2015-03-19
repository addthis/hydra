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

import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">applies left or right string padding</span>.
 * <p/>
 * <p>If the input string is shorter than the padding string, then the input
 * string is lengthened by filling in the characters from the padding. For example,
 * if the input is "Hello" and the left padding is "1234567" then the output is
 * "12Hello". If instead, the right padding is "1234567" then the output is "Hello67".</p>
 * <p>If you specify both left and right padding then the left padding is applied
 * first and then right padding is applied.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"SHARD", filter:{op:"chain", filter:[{op:"hash"},
 *               {op:"mod", mod:%[shards:32]%}, {op:"pad", left:"000"}]}},
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterPad extends StringFilter {

    /**
     * Apply left padding using this string.
     */
    @FieldConfig(codable = true)
    private String left;

    /**
     * Apply right padding using this string.
     */
    @FieldConfig(codable = true)
    private String right;

    @Override
    public String filter(String v) {
        if (v == null) {
            v = "";
        }
        if (left != null && v.length() < left.length()) {
            v = left.substring(v.length()).concat(v);
        }
        if (right != null && v.length() < right.length()) {
            v = v.concat(right.substring(v.length()));
        }
        return v;
    }
}

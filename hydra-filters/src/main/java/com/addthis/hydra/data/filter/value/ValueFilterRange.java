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
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">returns a substring of the string input</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    {op: "field", from: "FOO", to: "BAR", filter: {op: "range", start: 2}},
 * </pre>
 *
 * @user-reference
 * @hydra-name range
 */
public class ValueFilterRange extends StringFilter {

    /**
     * The start position of the substring in a 0-based offset (inclusive). Default is 0.
     */
    @FieldConfig(codable = true)
    private int start;

    /**
     * The end position of the substring in a 0-based offset (exclusive). Default is string length.
     */
    @FieldConfig(codable = true)
    private int end;

    public ValueFilterRange setRange(int start, int end) {
        this.start = start;
        this.end = end;
        return this;
    }

    @Override
    public String filter(String sv) {
        if (sv == null) {
            return sv;
        }
        int lv = sv.length();
        int s = start;
        int e = end;
        if (s < 0) {
            s = lv + s;
        }
        if (e <= 0) {
            e = lv + e;
        }
        if (s > lv) {
            s = lv;
        }
        if (e > lv) {
            e = lv;
        }
        return sv.substring(s, e);
    }

}

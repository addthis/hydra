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

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">splits the input string into an output string</span>.
 * <p/>
 * <p>The input string is interpreted as a sequence of items that are separated by
 * {@link #sep sep}. A subsequence of the input is extracted and the output string
 * is generated using the {@link #joinStr joinStr} separator.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"STR", to:”STR2”,  filter:{op:"string-slice", sep:"," , fromIndex:2}},
 * </pre>
 *
 * @user-reference
 * @hydra-name string-slice
 */
public class ValueFilterStringSlice extends StringFilter {

    /**
     * The input sequence string deliminator.
     */
    @FieldConfig(codable = true, required = true)
    private String sep;

    /**
     * The output sequence string deliminator. Defaults to {@link #sep sep} if joinStr is null.
     */
    @FieldConfig(codable = true)
    private String joinStr;

    /**
     * The start position of the sequence in a 0-based offset (inclusive). Default is 0.
     */
    @FieldConfig(codable = true)
    private Integer fromIndex;

    /**
     * The end position of the sequence in a 0-based offset (exclusive). Default is sequence length.
     */
    @FieldConfig(codable = true)
    private Integer toIndex;

    public ValueFilterStringSlice() {
    }

    protected ValueFilterStringSlice(String sep, int fromIndex) {
        this.sep = sep;
        this.fromIndex = fromIndex;
    }

    protected ValueFilterStringSlice(String sep, String joinStr, int fromIndex, int toIndex) {
        this.sep = sep;
        this.joinStr = joinStr;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
    }

    @Override
    public String filter(String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        if (joinStr == null) {
            joinStr = sep;
        }

        List<String> splitList = new ArrayList<String>();
        Iterators.addAll(splitList, Splitter.on(sep).split(value).iterator());

        if (toIndex == null) {
            toIndex = splitList.size();
        }

        if (fromIndex == null) {
            fromIndex = 0;
        }

        List<String> subList = splitList.subList(fromIndex, toIndex);
        return Joiner.on(joinStr).skipNulls().join(subList);
    }

}

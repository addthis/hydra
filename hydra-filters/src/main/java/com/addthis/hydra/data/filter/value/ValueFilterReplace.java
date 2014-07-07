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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">performs string replacement with optional regular expression matching</span>.
 * <p/>
 * <p>The default behavior is to perform literal string replacement. If the {@link #regex regex} field
 * is enabled, then the find pattern is interpreted as a regular expression.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"PAGE_URL", filter:{op:"replace",find:"ord=[0-9]*.[0-9]*",replace:"",regex:true}},
 * </pre>
 *
 * @user-reference
 * @hydra-name replace
 */
public class ValueFilterReplace extends StringFilter {

    /**
     * The matching string, interpreted either as a literal string or a regular expression.
     */
    @FieldConfig(codable = true)
    private String find;

    /**
     * The replacement string.
     */
    @FieldConfig(codable = true)
    private String replace;

    /**
     * If true, then interpret 'find' as a regular expression. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean regex;

    private Pattern pattern;

    public ValueFilterReplace setFind(String find) {
        this.find = find;
        return this;
    }

    public ValueFilterReplace setReplace(String replace) {
        this.replace = replace;
        return this;
    }

    public ValueFilterReplace setRegex(boolean regex) {
        this.regex = regex;
        return this;
    }

    @Override
    public String filter(String value) {
        if (value != null) {
            if (pattern == null) {
                if (regex) {
                    pattern = Pattern.compile(find);
                } else {
                    pattern = Pattern.compile(find, Pattern.LITERAL);
                    replace = Matcher.quoteReplacement(replace);
                }
            }
            value = pattern.matcher(value).replaceAll(replace);
        }
        return value;
    }

}

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    private final String find;

    /**
     * The replacement string.
     */
    private final String replace;

    /**
     * If true, then interpret 'find' as a regular expression. Default is false.
     */
    private final boolean regex;

    private final Pattern pattern;

    @JsonCreator
    public ValueFilterReplace(@JsonProperty("find") String find,
                              @JsonProperty("replace") String replace,
                              @JsonProperty("regex") boolean regex) {
        this.find = find;
        this.regex = regex;
        if (regex) {
            this.pattern = (find != null) ? Pattern.compile(find) : null;
            this.replace = replace;
        } else {
            this.pattern = (find != null) ? Pattern.compile(find, Pattern.LITERAL) : null;
            this.replace = (replace != null) ? Matcher.quoteReplacement(replace) : null;
        }
    }

    @Override
    public void open() { }

    @Override
    public String filter(String value) {
        if (value != null) {
            value = pattern.matcher(value).replaceAll(replace);
        }
        return value;
    }

}

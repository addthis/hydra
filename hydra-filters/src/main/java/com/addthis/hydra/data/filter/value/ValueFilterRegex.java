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

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">performs regular expression matching on the input string</span>.
 * <p/>
 * <p>The default behavior is to perform regular expression matching on the input string,
 * and return an array with all the substrings that match to the regular expression.
 * If the {@link #replace replace} field is used, then all substrings that match against
 * the pattern are replaced with the replacement string and the output is a string.
 * <p/>
 * <p>The regex specification used by hydra (similar to perl 5) can be found <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">here</a>.
 * Once you design a regex to match the desired pattern, you must encode it in a JSON string.
 * Notably, this means your pattern must be wrapped in double-quotes ("), and JSON string special characters, backslash (\), single-quote ('), and double-quote(") must be escaped by prefixing with a backslash.
 * Consider the follow examples:
 * <pre>
 *   1932741273804          desired text to match
 *   \d+                    regex pattern, using \d to match numeric characters
 *   "\\d+"                 pattern encoded as JSON string, quote surrounded and \ escaped by \
 *   {op: "regex", pattern: "\\d+", replace: "string of numbers"}
 * </pre>
 * <pre>
 *   "C:\WINDOWS"           desired text to match (match enclosing quotes as well)
 *   "C:\\WINDOWS"          regex pattern, \ is escaped by \ according to regex standard
 *   "\"C:\\\\WINDOWS\""    pattern encoded as JSON string, quote surrounded with \ and " escaped by \
 *   {op: "regex", pattern: "\"C:\\\\WINDOWS\"", replace: "LINUX"}
 * </pre>
 * </p>
 * <p>Example:</p>
 * <pre>
 *   {from:"SOURCE", to:"SOURCE", filter:{op:"regex", pattern:"Log_([0-9]+)\\."}},
 * </pre>
 *
 * @user-reference
 * @hydra-name regex
 */
public class ValueFilterRegex extends AbstractValueFilter {

    /**
     * Regular expression to match against. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private Pattern pattern;

    /**
     * If non-null, then replace all matches with this string. Default is null.
     */
    @FieldConfig(codable = true)
    private String replace;

    public ValueFilterRegex setPattern(Pattern p) {
        pattern = p;
        return this;
    }

    public ValueFilterRegex setReplace(String r) {
        replace = r;
        return this;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        String sv = ValueUtil.asNativeString(value);
        if (sv == null) {
            return null;
        }
        Matcher matcher = pattern.matcher(sv);
        if (replace != null) {
            return ValueFactory.create(matcher.replaceAll(replace));
        }
        ValueArray arr = ValueFactory.createArray(1);
        while (matcher.find()) {
            int count = matcher.groupCount();
            for (int i = 1; i <= count; i++) {
                arr.add(ValueFactory.create(matcher.group(i)));
            }
        }
        return arr;
    }

}

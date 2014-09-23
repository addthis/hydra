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
import java.util.HashSet;
import java.util.regex.Pattern;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.util.JSONFetcher;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">filters the input based on one or more string-matching criteria</span>.
 * <p/>
 * <p>One or more of the following filtering fields can be used:</p>
 * <p>
 * <ul>
 * <li>{@link #value value} - the input must match exactly to an element in this set.</li>
 * <li>{@link #contains contains} - a substring of the input must match exactly to an element of this set.</li>
 * <li>{@link #match match} - the input must match to one of the regular expressions in this set.</li>
 * <li>{@link #find find} - a substring of the input must match to one of the regular expressions in this set.</li>
 * </ul>
 * </p>
 * <p>The URL fields can be used to retrieve the search filters from the Internet. If more than one field is used,
 * then the input must pass at least one filter to be accepted.</p>
 * <p>If the input string is successful on the filtering criteria then the string is returned as the output.
 * Otherwise a null value is returned. If the input is null or empty then the output
 * is null or empty respectively. To eliminate empty values,
 * either wrap this filter inside of a {@link ValueFilterChain chain} filter or use the negation of
 * an {@link ValueFilterEmpty empty} filter.
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"ID", filter:{op:"require", match:["[0-9A-Fa-f]+"]}},
 * </pre>
 *
 * @user-reference
 * @hydra-name require
 */
public class ValueFilterRequire extends StringFilter {

    /**
     * The input must match exactly to an element in this set.
     */
    @FieldConfig(codable = true)
    private HashSet<String> value;

    /**
     * A URL to retrieve the 'value' field.
     */
    @FieldConfig(codable = true)
    private String valueURL;

    /**
     * The input must match to one of the regular expressions in this set.
     */
    @FieldConfig(codable = true)
    private HashSet<String> match;

    /**
     * A URL to retrieve the 'match' field.
     */
    @FieldConfig(codable = true)
    private String matchURL;

    /**
     * A substring of the input must match to one of the regular expressions in this set.
     */
    @FieldConfig(codable = true)
    private HashSet<String> find;

    /**
     * A URL to retrieve the 'find' field.
     */
    @FieldConfig(codable = true)
    private String findURL;

    /**
     * A substring of the input must match exactly to an element of this set.
     */
    @FieldConfig(codable = true)
    private String[] contains;

    /**
     * A URL to retrieve the 'contains' field.
     */
    @FieldConfig(codable = true)
    private String containsURL;

    /**
     * If true, then interpret the payload from the URLs as CSV files. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean urlReturnsCSV;

    /**
     * If true, then convert the input to lowercase. The filter output will be in lowercase.
     * Default is false.
     */
    @FieldConfig(codable = true)
    private boolean toLower;

    /**
     * A timeout value if any of the URL fields are used. Default is 60000.
     */
    @FieldConfig(codable = true)
    private int urlTimeout = 60000;

    /**
     * The number of retries if any of the URL fields are used. Default is 5.
     */
    @FieldConfig(codable = true)
    private int urlRetries = 5;

    private ArrayList<Pattern> pattern;
    private ArrayList<Pattern> findPattern;

    public ValueFilterRequire setValue(HashSet<String> value) {
        this.value = value;
        return this;
    }

    public ValueFilterRequire setMatch(HashSet<String> match) {
        this.match = match;
        return this;
    }

    public ValueFilterRequire setContains(String[] contains) {
        this.contains = contains;
        return this;
    }

    public ValueFilterRequire setFind(HashSet<String> find) {
        this.find = find;
        return this;
    }

    public ValueFilterRequire setToLower(boolean toLower) {
        this.toLower = toLower;
        return this;
    }

    public boolean passedMatch(String sv) {
        // match regex
        if (pattern != null) {
            for (Pattern pat : pattern) {
                if (pat.matcher(sv).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean passedContains(String sv) {
        // match contains
        if (contains != null) {
            for (String search : contains) {
                if (sv.indexOf(search) >= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean passedValue(String sv) {
        // match exact values
        if (value != null && value.contains(sv)) {
            return true;
        }
        return false;
    }

    public boolean passedFind(String sv) {
        // match regex
        if (findPattern != null) {
            for (Pattern pat : findPattern) {
                if (pat.matcher(sv).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void setup() {
        if (valueURL != null) {
            if (urlReturnsCSV) {
                value = JSONFetcher.staticLoadCSVSet(valueURL, urlTimeout, urlRetries, value);
            } else {
                value = JSONFetcher.staticLoadSet(valueURL, urlTimeout, urlRetries, value);
            }
        }
        if (matchURL != null) {
            if (urlReturnsCSV) {
                match = JSONFetcher.staticLoadCSVSet(matchURL, urlTimeout, urlRetries, match);
            } else {
                match = JSONFetcher.staticLoadSet(matchURL, urlTimeout, urlRetries, match);
            }
        }
        if (findURL != null) {
            if (urlReturnsCSV) {
                find = JSONFetcher.staticLoadCSVSet(findURL, urlTimeout, urlRetries, find);
            } else {
                find = JSONFetcher.staticLoadSet(findURL, urlTimeout, urlRetries, find);
            }
        }
        if (containsURL != null) {
            HashSet<String> tmp = null;

            if (urlReturnsCSV) {
                tmp = JSONFetcher.staticLoadCSVSet(containsURL, urlTimeout, urlRetries, tmp);
            } else {
                tmp = JSONFetcher.staticLoadSet(containsURL);
            }

            contains = tmp.toArray(new String[tmp.size()]);
        }
        if (match != null) {
            ArrayList<Pattern> np = new ArrayList<>();
            for (String s : match) {
                np.add(Pattern.compile(s));
            }
            this.pattern = np;
        }
        if (find != null) {
            ArrayList<Pattern> np = new ArrayList<>();
            for (String s : find) {
                np.add(Pattern.compile(s));
            }
            this.findPattern = np;
        }
    }

    @Override
    public String filter(String sv) {
        requireSetup();
        if (sv != null && !sv.equals("")) {
            if (toLower) {
                sv = sv.toLowerCase();
            }
            if (passedMatch(sv) || passedContains(sv) || passedValue(sv) || passedFind(sv)) {
                return sv;
            } else {
                return null;
            }
        }
        return sv;
    }

}

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">excludes matching values</span>.
 * <p/>
 * <p>This filter contains a number of fields. Each field performs a different type of matching.
 * If more that one field is used, then the input must match for all of the specified fields in
 * order to be accepted.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {op:"field", from:"USER", filter:{op:"exclude", value:["0"]}},
 * </pre>
 *
 * @user-reference
 * @hydra-name exclude
 */
public class ValueFilterExclude extends StringFilter {

    /**
     * A set of strings. The input must be an exact match to a member of this set to be accepted.
     */
    private HashSet<String> value;

    /**
     * A url that points to a set of strings that are used in place of the {@link #value value}
     * field.
     */
    final private String valueURL;

    /**
     * A set of regular expression strings. The entire input must match against a regular
     * expression to be accepted.
     */
    private HashSet<String> match;

    /**
     * A url that points to a set of strings that are used in place of the {@link #match match}
     * field.
     */
    final private String matchURL;

    /**
     * A set of regular expression strings. The substring of the input must be found in a regular
     * expression to be accepted.
     */
    private HashSet<String> find;

    /**
     * A url that points to a set of strings that are used in place of the {@link #find find} field.
     */
    final private String findURL;

    /**
     * A set of strings. The input must a substring of a member of the set to be accepted.
     */
    private String[] contains;

    /**
     * A url that points to a set of strings that are used in place of the {@link #contains
     * contains} field.
     */
    final private String containsURL;

    /**
     * A timeout if any of the url fields are used.
     */
    final private int urlTimeout;

    /**
     * The number of connection retries if any of the url fields are used.
     */
    final private int urlRetries;

    private ArrayList<Pattern> pattern;
    private ArrayList<Pattern> findPattern;

    @JsonCreator
    public ValueFilterExclude(@JsonProperty("value") HashSet<String> value,
                              @JsonProperty("valueURL") String valueURL,
                              @JsonProperty("match") HashSet<String> match,
                              @JsonProperty("matchURL") String matchURL,
                              @JsonProperty("find") HashSet<String> find,
                              @JsonProperty("findURL") String findURL,
                              @JsonProperty("contains") String[] contains,
                              @JsonProperty("containsURL") String containsURL,
                              @JsonProperty("urlTimeout") int urlTimeout,
                              @JsonProperty("urlRetries") int urlRetries) {
        this.value = value;
        this.valueURL = valueURL;
        this.match = match;
        this.matchURL = matchURL;
        this.find = find;
        this.findURL = findURL;
        this.contains = contains;
        this.containsURL = containsURL;
        this.urlTimeout = urlTimeout;
        this.urlRetries = urlRetries;
        if (match != null) {
            ArrayList<Pattern> newpat = new ArrayList<>();
            for (String s : match) {
                newpat.add(Pattern.compile(s));
            }
            pattern = newpat;
        }
        if (find != null) {
            ArrayList<Pattern> newpat = new ArrayList<>();
            for (String s : find) {
                newpat.add(Pattern.compile(s));
            }
            findPattern = newpat;
        }
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

    @Override
    public void open() {
        if (valueURL != null) {
            value = JSONFetcher.staticLoadSet(valueURL, urlTimeout, urlRetries, value);
        }
        if (matchURL != null) {
            match = JSONFetcher.staticLoadSet(matchURL, urlTimeout, urlRetries, match);
            if (match != null) {
                ArrayList<Pattern> newpat = new ArrayList<>();
                for (String s : match) {
                    newpat.add(Pattern.compile(s));
                }
                pattern = newpat;
            }
        }
        if (findURL != null) {
            find = JSONFetcher.staticLoadSet(findURL, urlTimeout, urlRetries, find);
            if (find != null) {
                ArrayList<Pattern> newpat = new ArrayList<>();
                for (String s : find) {
                    newpat.add(Pattern.compile(s));
                }
                findPattern = newpat;
            }
        }
        if (containsURL != null) {
            HashSet<String> tmp = JSONFetcher.staticLoadSet(containsURL);
            contains = tmp.toArray(new String[tmp.size()]);
        }
    }

    @Override
    public String filter(String v) {
        if (v != null) {
            String sv = v.toString();
            if (passedMatch(sv) || passedContains(sv) || passedValue(sv) || passedFind(sv)) {
                return null;
            } else {
                return sv;
            }
        }
        return v;
    }

}

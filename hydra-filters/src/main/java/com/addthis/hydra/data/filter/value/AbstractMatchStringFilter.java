package com.addthis.hydra.data.filter.value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.util.JSONFetcher;

import com.fasterxml.jackson.annotation.JsonProperty;

abstract class AbstractMatchStringFilter extends StringFilter implements SuperCodable {

    /**
     * The input must match exactly to an element in this set.
     */
    private HashSet<String> value;

    /**
     * A URL to retrieve the 'value' field.
     */
    final private String valueURL;

    /**
     * The input must match to one of the regular expressions in this set.
     */
    private HashSet<String> match;

    /**
     * A URL to retrieve the 'match' field.
     */
    final private String matchURL;

    /**
     * A substring of the input must match to one of the regular expressions in this set.
     */
    private HashSet<String> find;

    /**
     * A URL to retrieve the 'find' field.
     */
    final private String findURL;

    /**
     * A substring of the input must match exactly to an element of this set.
     */
    private String[] contains;

    /**
     * A URL to retrieve the 'contains' field.
     */
    final private String containsURL;

    /**
     * If true, then interpret the payload from the URLs as CSV files. Default is false.
     */
    final private boolean urlReturnsCSV;

    /**
     * If true, then convert the input to lowercase. The filter output will be in lowercase.
     * Default is false.
     */
    final private boolean toLower;

    /**
     * A timeout value if any of the URL fields are used. Default is 60000.
     */
    final private int urlTimeout;

    /**
     * The number of retries if any of the URL fields are used. Default is 5.
     */
    final private int urlRetries;

    final private boolean not;

    private ArrayList<Pattern> pattern;
    private ArrayList<Pattern> findPattern;

    AbstractMatchStringFilter(HashSet<String> value,
                              String valueURL,
                              HashSet<String> match,
                              String matchURL,
                              HashSet<String> find,
                              String findURL,
                              String[] contains,
                              String containsURL,
                              boolean urlReturnsCSV,
                              boolean toLower,
                              int urlTimeout,
                              int urlRetries,
                              boolean not) {
        this.value = value;
        this.valueURL = valueURL;
        this.match = match;
        this.matchURL = matchURL;
        this.find = find;
        this.findURL = findURL;
        this.contains = contains;
        this.containsURL = containsURL;
        this.urlReturnsCSV = urlReturnsCSV;
        this.toLower = toLower;
        this.urlTimeout = urlTimeout;
        this.urlRetries = urlRetries;
        this.not = not;
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

    @Override public void postDecode() {
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
            if (match != null) {
                ArrayList<Pattern> np = new ArrayList<>();
                for (String s : match) {
                    np.add(Pattern.compile(s));
                }
                this.pattern = np;
            }
        }
        if (findURL != null) {
            if (urlReturnsCSV) {
                find = JSONFetcher.staticLoadCSVSet(findURL, urlTimeout, urlRetries, find);
            } else {
                find = JSONFetcher.staticLoadSet(findURL, urlTimeout, urlRetries, find);
            }
            if (find != null) {
                ArrayList<Pattern> np = new ArrayList<>();
                for (String s : find) {
                    np.add(Pattern.compile(s));
                }
                this.findPattern = np;
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
    }

    @Override public void preEncode() {}

    private static final class ValidationOnly extends AbstractMatchStringFilter {
        public ValidationOnly(@JsonProperty("value") HashSet<String> value,
                              @JsonProperty("valueURL") String valueURL,
                              @JsonProperty("match") HashSet<String> match,
                              @JsonProperty("matchURL") String matchURL,
                              @JsonProperty("find") HashSet<String> find,
                              @JsonProperty("findURL") String findURL,
                              @JsonProperty("contains") String[] contains,
                              @JsonProperty("containsURL") String containsURL,
                              @JsonProperty("urlReturnsCSV") boolean urlReturnsCSV,
                              @JsonProperty("toLower") boolean toLower,
                              @JsonProperty("urlTimeout") int urlTimeout,
                              @JsonProperty("urlRetries") int urlRetries) {
            super(value,
                  valueURL,
                  match,
                  matchURL,
                  find,
                  findURL,
                  contains,
                  containsURL,
                  urlReturnsCSV,
                  toLower,
                  urlTimeout,
                  urlRetries,
                  false);
        }

        @Override public void postDecode() {
            // intentionally do nothing
        }

        @Override public ValueObject filter(ValueObject value) {
            throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
        }
    }

    @Override
    public String filter(String sv) {
        if (sv != null && (not || !sv.equals(""))) {
            if (toLower) {
                sv = sv.toLowerCase();
            }
            boolean success = passedMatch(sv) || passedContains(sv) || passedValue(sv) || passedFind(sv);
            if (not) {
                return success ? null : sv;
            } else {
                return success ? sv : null;
            }
        }
        return sv;
    }

}

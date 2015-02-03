package com.addthis.hydra.data.filter.value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;

import com.addthis.hydra.data.util.JSONFetcher;

abstract class AbstractMatchStringFilter extends StringFilter {

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

    public AbstractMatchStringFilter(HashSet<String> value,
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

    @Override
    public void open() {
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

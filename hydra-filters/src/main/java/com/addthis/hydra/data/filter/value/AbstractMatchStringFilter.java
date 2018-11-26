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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.addthis.ahocorasick.AhoCorasick;
import com.addthis.ahocorasick.SearchResult;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoParam;
import com.addthis.bundle.util.ConstantTypedField;
import com.addthis.bundle.util.TypedField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Time;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.util.JSONFetcher;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.MoreExecutors;

abstract class AbstractMatchStringFilter extends AbstractValueFilterContextual implements SuperCodable {

    /**
     * The input must match exactly to an element in this set.
     */
    private AtomicReference<TypedField<Set<String>>> value;

    /**
     * A URL to retrieve the 'value' field.
     */
    final private String valueURL;

    /**
     * The input must match to one of the regular expressions in this set.
     */
    private volatile HashSet<String> match;

    /**
     * A URL to retrieve the 'match' field.
     */
    final private String matchURL;

    /**
     * A substring of the input must match to one of the regular expressions in this set.
     */
    private volatile HashSet<String> find;

    /**
     * A URL to retrieve the 'find' field.
     */
    final private String findURL;

    /**
     * A substring of the input must match exactly to an element of this set.
     */
    private AtomicReference<TypedField<Set<String>>> contains;

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

    /**
     * Set an interval to fetch the required URL and update the bundle filter setting by calling the postDecodeHelper.
     */
    final private int refreshMinutes;

    final private int urlMinBackoff;

    final private int urlMaxBackoff;

    final private boolean not;

    private AtomicReference<ArrayList<Pattern>> pattern = new AtomicReference<>();
    private AtomicReference<ArrayList<Pattern>> findPattern = new AtomicReference<>();

    private AtomicReference<AhoCorasick> containsDictionary = new AtomicReference<>();

    AbstractMatchStringFilter(TypedField<Set<String>> value,
                              String valueURL,
                              HashSet<String> match,
                              String matchURL,
                              HashSet<String> find,
                              String findURL,
                              TypedField<Set<String>> contains,
                              String containsURL,
                              boolean urlReturnsCSV,
                              boolean toLower,
                              int urlTimeout,
                              int urlRetries,
                              int refreshMinutes,
                              int urlMinBackoff,
                              int urlMaxBackoff,
                              boolean not) {
        this.value = new AtomicReference<>(value);
        this.valueURL = valueURL;
        this.match = match;
        this.matchURL = matchURL;
        this.find = find;
        this.findURL = findURL;
        this.contains = new AtomicReference<>(contains);
        this.containsURL = containsURL;
        this.urlReturnsCSV = urlReturnsCSV;
        this.toLower = toLower;
        this.urlTimeout = urlTimeout;
        this.urlRetries = urlRetries;
        this.refreshMinutes = refreshMinutes;
        this.urlMinBackoff = urlMinBackoff;
        this.urlMaxBackoff = urlMaxBackoff;
        this.not = not;
        if (match != null) {
            ArrayList<Pattern> np = new ArrayList<>();
            for (String s : match) {
                np.add(Pattern.compile(s));
            }
            this.pattern.set(np);
        }
        if (find != null) {
            ArrayList<Pattern> np = new ArrayList<>();
            for (String s : find) {
                np.add(Pattern.compile(s));
            }
            this.findPattern.set(np);
        }
    }

    public boolean passedMatch(String sv) {
        // match regex
        ArrayList<Pattern> localPattern = pattern.get();
        if (localPattern != null) {
            for (Pattern pat : localPattern) {
                if (pat.matcher(sv).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean passedContains(String sv, Bundle context) {
        // match contains
        AhoCorasick localContainsDictionary = containsDictionary.get();
        TypedField<Set<String>> localContains = contains.get();
        if (localContainsDictionary != null) {
            Iterator<SearchResult> matcher = localContainsDictionary.progressiveSearch(sv);
            return matcher.hasNext();
        } else if (localContains != null) {
            for (String search : localContains.getValue(context)) {
                if (sv.contains(search)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean passedValue(String sv, Bundle context) {
        // match exact values
        TypedField<Set<String>> localValue = value.get();
        return (localValue != null) && localValue.getValue(context).contains(sv);
    }

    public boolean passedFind(String sv) {
        // match regex
        ArrayList<Pattern> localFindPattern = findPattern.get();
        if (localFindPattern != null) {
            for (Pattern pat : localFindPattern) {
                if (pat.matcher(sv).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void postDecodeHelper() {
        if (valueURL != null) {
            JSONFetcher.SetLoader loader = new JSONFetcher.SetLoader(valueURL)
                    .setContention(urlTimeout, urlRetries, urlMinBackoff, urlMaxBackoff);
            if (urlReturnsCSV) {
                loader.setCsv(true);
            }
            value.set(new ConstantTypedField<>(loader.load()));
        }
        if (matchURL != null) {
            JSONFetcher.SetLoader loader = new JSONFetcher.SetLoader(matchURL)
                    .setContention(urlTimeout, urlRetries, urlMinBackoff, urlMaxBackoff).setTarget(match);
            if (urlReturnsCSV) {
                loader.setCsv(true);
            }
            match = loader.load();
            if (match != null) {
                ArrayList<Pattern> np = new ArrayList<>();
                for (String s : match) {
                    np.add(Pattern.compile(s));
                }
                this.pattern.set(np);
            }
        }
        if (findURL != null) {
            JSONFetcher.SetLoader loader = new JSONFetcher.SetLoader(findURL)
                    .setContention(urlTimeout, urlRetries, urlMinBackoff, urlMaxBackoff).setTarget(find);
            if (urlReturnsCSV) {
                loader.setCsv(true);
            }
            find = loader.load();
            if (find != null) {
                ArrayList<Pattern> np = new ArrayList<>();
                for (String s : find) {
                    np.add(Pattern.compile(s));
                }
                this.findPattern.set(np);
            }
        }
        if (containsURL != null) {
            JSONFetcher.SetLoader loader = new JSONFetcher.SetLoader(containsURL)
                    .setContention(urlTimeout, urlRetries, urlMinBackoff, urlMaxBackoff);
            if (urlReturnsCSV) {
                loader.setCsv(true);
            }
            contains.set(new ConstantTypedField<>(loader.load()));
        }
        TypedField<Set<String>> nc = contains.get();
        if (nc instanceof Supplier) {
            Set<String> candidates = ((Supplier<Set<String>>) nc).get();
            if (candidates != null) {
                AhoCorasick nd = AhoCorasick.builder().build();
                candidates.forEach(nd::add);
                nd.prepare();
                containsDictionary.set(nd);
            }
        }
    }

    @Override public void postDecode() {
        // need to call postDecodeHelper first even in the scheduled service case. Otherwise the filter might not be initialized since
        // the new thread might not be ready to finish decoding, and the current thread might use the uninitialized object
        postDecodeHelper();
        if (refreshMinutes > 0) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            // upon JVM termination, wait for the tasks for up to 100ms before exiting the executor service
            ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(executor, 100, TimeUnit.MILLISECONDS);
            executorService.scheduleWithFixedDelay(this :: postDecodeHelper, refreshMinutes, refreshMinutes, TimeUnit.MINUTES);
        }
    }

    @Override public void preEncode() {}

    private static final class ValidationOnly extends AbstractMatchStringFilter {
        public ValidationOnly(@AutoParam @JsonProperty("value") TypedField<Set<String>> value,
                              @JsonProperty("valueURL") String valueURL,
                              @JsonProperty("match") HashSet<String> match,
                              @JsonProperty("matchURL") String matchURL,
                              @JsonProperty("find") HashSet<String> find,
                              @JsonProperty("findURL") String findURL,
                              @AutoParam @JsonProperty("contains") TypedField<Set<String>> contains,
                              @JsonProperty("containsURL") String containsURL,
                              @JsonProperty("urlReturnsCSV") boolean urlReturnsCSV,
                              @JsonProperty("toLower") boolean toLower,
                              @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlTimeout") int urlTimeout,
                              @JsonProperty("urlRetries") int urlRetries,
                              @Time(TimeUnit.MINUTES) @JsonProperty("refreshMinutes") int refreshMinutes,
                              @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlMinBackoff") int urlMinBackoff,
                              @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlMaxBackoff") int urlMaxBackoff) {
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
                  refreshMinutes,
                  urlMinBackoff,
                  urlMaxBackoff,
                  false);
        }

        @Override public void postDecode() {
            // intentionally do nothing
        }

        @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
            throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
        }
    }

    @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
        String sv = (value == null) ? null : value.toString();
        if (sv != null && (not || !sv.isEmpty())) {
            if (toLower) {
                sv = sv.toLowerCase();
                value = ValueFactory.create(sv);
            }
            boolean success = passedMatch(sv) || passedContains(sv, context) || passedValue(sv, context) || passedFind(sv);
            if (not) {
                return success ? null : value;
            } else {
                return success ? value : null;
            }
        } else {
            return value;
        }
    }

}
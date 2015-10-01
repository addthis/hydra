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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.util.TypedField;
import com.addthis.codec.annotations.Time;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">filters the input based on one or more string-matching criteria</span>.
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
 *     {from:"ID", require.match:["[0-9A-Fa-f]+"]}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterRequire extends AbstractMatchStringFilter {

    @JsonCreator @VisibleForTesting
    ValueFilterRequire(@JsonProperty("value") TypedField<Set<String>> value,
                       @JsonProperty("valueURL") String valueURL,
                       @JsonProperty("match") HashSet<String> match,
                       @JsonProperty("matchURL") String matchURL,
                       @JsonProperty("find") HashSet<String> find,
                       @JsonProperty("findURL") String findURL,
                       @JsonProperty("contains") TypedField<Set<String>> contains,
                       @JsonProperty("containsURL") String containsURL,
                       @JsonProperty("urlReturnsCSV") boolean urlReturnsCSV,
                       @JsonProperty("toLower") boolean toLower,
                       @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlTimeout") int urlTimeout,
                       @JsonProperty("urlRetries") int urlRetries,
                       @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlMinBackoff") int urlMinBackoff,
                       @Time(TimeUnit.MILLISECONDS) @JsonProperty("urlMaxBackoff") int urlMaxBackoff
    ) {
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
              urlMinBackoff,
              urlMaxBackoff,
              false);
    }

}

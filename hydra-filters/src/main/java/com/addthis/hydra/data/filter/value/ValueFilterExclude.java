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
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">excludes matching values</span>.
 * <p/>
 * <p>This filter contains a number of fields. Each field performs a different type of matching.
 * If more that one field is used, then the input must match for all of the specified fields in
 * order to be accepted.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {from:"USER", exclude:"0"}},
 *      {from:"USER", exclude.contains:["0","1"]}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterExclude extends AbstractMatchStringFilter {

    @JsonCreator @VisibleForTesting
    ValueFilterExclude(@JsonProperty("value") TypedField<Set<String>> value,
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
              true);
    }

}

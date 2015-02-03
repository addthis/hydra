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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.arabidopsis.ahocorasick.AhoCorasick;
import org.arabidopsis.ahocorasick.SearchResult;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">checks for strings, arrays or maps
 * that contain the target keys or values</span>.
 * <p/>
 * <p>If the input is a string then return the input if it is a substring of an element in {@link #value value},
 * or otherwise return null.
 * <p>If the input is an array and {@link #once once} is {@code false} (the default value of the once
 * parameter is {@code false}), then iterate over each element of the array and
 * test if it is a substring of an element in {@link #value value}. If the input is an array and
 * once is {@code true}, then return the input if at least one element of the input exactly matches
 * an element in {@link #value value}. If the input is an array and once is {@code true},
 * then return either the entire input or otherwise return null.
 * <p>If the input is a map and once is either {@code true} or {@code false}, then return the entire map
 * if at least one key exactly matches an element of {@link #key key} or
 * one value exactly matches an element of {@link #value value}.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {from:"EVT", to:"VALID", filter:{op:"contains", key:["90_typ","90_rsc","90_rsi"]}},
 * </pre>
 *
 * @user-reference
 * @hydra-name contains
 */
public class ValueFilterContains extends AbstractValueFilter {

    /**
     * The set of values to match against.
     */
    final private String[] value;

    /**
     * The set of keys to match against. Only applicable for map inputs.
     */
    final private String[] key;

    /**
     * If true then return values that do not match. Default is false.
     */
    final private boolean not;

    /**
     * If true then matched value is returned v/s the input
     */
    final private boolean returnMatch;

    final private AhoCorasick dictionary;

    @JsonCreator
    public ValueFilterContains(@JsonProperty("value") String[] value,
                               @JsonProperty("key") String[] key,
                               @JsonProperty("not") boolean not,
                               @JsonProperty("returnMatch") boolean returnMatch) {
        this.value = value;
        this.key = key;
        this.not = not;
        this.returnMatch = returnMatch;
        this.dictionary = (value != null) ? new AhoCorasick() : null;
        if (dictionary != null) {
            for (String pattern : value) {
                dictionary.add(pattern);
            }
            dictionary.prepare();
        }
    }

    @Override
    public ValueObject filterValue(ValueObject input) {

        if (input == null) {
            return null;
        }

        String match;
        ValueObject.TYPE type = input.getObjectType();
        if (type == ValueObject.TYPE.MAP) {
            ValueMap inputAsMap = input.asMap();
            for (String cmp : inputAsMap.keySet()) {
                if (key != null && (match = checkContains(cmp, key)) != null) {
                    return not ? null : returnMatch ? ValueFactory.create(match) : input;
                }
                if (value != null && (match = checkContains(cmp, value)) != null) {
                    return not ? null : returnMatch ? ValueFactory.create(match) : input;
                }
            }
        } else if (type == ValueObject.TYPE.ARRAY && value != null) {
            for (ValueObject el : input.asArray()) {
                String cmp = ValueUtil.asNativeString(el);
                if ((match = checkContains(cmp, value)) != null) {
                    return not ? null : returnMatch ? ValueFactory.create(match) : input;
                }
            }
        } else if (type == ValueObject.TYPE.STRING && value != null) {
            return compareInputAsString(input);
        }

        return not ? input : null;
    }

    @Nullable
    private ValueObject compareInputAsString(@Nonnull ValueObject input) {
        assert (input.getObjectType() == ValueObject.TYPE.STRING);
        assert (value != null);

        String cmp = ValueUtil.asNativeString(input);

        Iterator<SearchResult> matcher = dictionary.progressiveSearch(cmp);

        if (matcher.hasNext()) {
            if (not) {
                return null;
            } else if (returnMatch) {
                return ValueFactory.create((String) matcher.next().getOutputs().iterator().next());
            } else {
                return input;
            }
        }

        return not ? input : null;
    }

    private String checkContains(String cmp, String[] arr) {
        for (String val : arr) {
            if (cmp.equals(val)) {
                return val;
            }
        }
        return null;
    }
}

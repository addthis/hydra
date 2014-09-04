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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">accepts a map as input and then performs
 * filtering operations on the input</span>.
 * <p/>
 * <p>If the {@link #whitelist whitelist} field is non-null, then any keys outside
 * of the whitelist are discarded. If the {@link #blacklist blacklist} field is non-null,
 * then any keys that are in the blacklist are discarded. By default a map is returned
 * as output. The {@link #toString toString} field can be used to emit a string as output.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     // assume PARAM="k1=v1,k2=v2,k3=v3"
 *
 *     {op:"field", from:"PARAM", filter:{op:"split", split:"=", keySplit:","}},
 *
 *     // this will give you the subset of the map
 *     // returns MAP[k1 -> v1, k2 -> v2 ]
 *
 *     {op:"field", from:"PARAM", to:"MAP", filter:{op:"map-subset",
 *         whitelist:["k1,"k2"]}},
 *
 *     // this will give you the subset of the map as a string
 *     // returns "k1=v1,k2=v2"
 *
 *     {op:"field", from:"PARAM", to:"MAP", filter:{op:"map-subset",
 *         whitelist:["k1","k2"], toString:true, keySep:"=", valueSep:","}},
 * </pre>
 *
 * @user-reference
 * @hydra-name map-subset
 */
public class ValueFilterMapSubset extends ValueFilter {
    private static final Logger log = LoggerFactory.getLogger(ValueFilterMapSubset.class);

    /** Set of keys that are preserved by the filter. */
    @JsonProperty private String[] whitelist;

    /** Set of keys that are excluded by the filter. */
    @JsonProperty private String[] blacklist;

    /** If true, then convert the output to a string. Default is false. */
    @JsonProperty private boolean toString;

    /** If toString is true, then use this field as the delimiter between a key and a value. */
    @JsonProperty private String keySep = "=";

    /** If toString is true, then use this field as the deliminator between two (key,value) pairs. */
    @JsonProperty private String valueSep = ",";


    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return null;
        }
        ValueMap<?> map;
        try {
            map = value.asMap();
        } catch (ValueTranslationException vte) {
            log.warn("Error extracting map from value: {}", value);
            return null;
        }

        ValueMap subsetMap;
        if ((whitelist != null) && (whitelist.length > 0)) {
            subsetMap = ValueFactory.createMap();
            for (String key : whitelist) {
                ValueObject<?> valueObject = map.get(key);
                if (valueObject != null) {
                    subsetMap.put(key, valueObject);
                }
            }
        } else {
            subsetMap = map;
        }

        if ((blacklist != null) && (blacklist.length > 0)) {
            for (String key : blacklist) {
                subsetMap.remove(key);
            }
        }

        if (toString) {
            return toString(subsetMap);
        } else {
            return subsetMap;
        }
    }

    private ValueString toString(ValueMap<?> subsetMap) {
        StringBuilder sb = new StringBuilder();

        for (ValueMapEntry valueMapEntry : subsetMap) {
            if (sb.length() == 0) {
                sb.append(valueMapEntry.getKey())
                  .append(keySep)
                  .append(valueMapEntry.getValue());
            } else {
                sb.append(valueSep)
                  .append(valueMapEntry.getKey())
                  .append(keySep)
                  .append(valueMapEntry.getValue());
            }
        }

        return ValueFactory.create(sb.toString());
    }
}

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
import com.addthis.codec.Codec;

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

    /**
     * A set of keys that are preserved by the filter.
     */
    @Codec.Set(codable = true)
    private String[] whitelist;

    /**
     * A set of keys that are excluded by the filter.
     */
    @Codec.Set(codable = true)
    private String[] blacklist;

    /**
     * If true, then convert the output to a string. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean toString;

    /**
     * If toString is true, then use this field as the delimiter between a key and a value.
     */
    @Codec.Set(codable = true)
    private String keySep = "=";

    /**
     * If toString is true, then use this field as the deliminator between two (key,value) pairs.
     */
    @Codec.Set(codable = true)
    private String valueSep = ",";

    private static final Logger log = LoggerFactory.getLogger(ValueFilterMapSubset.class);

    @Override
    public ValueObject filterValue(ValueObject value) {
        ValueObject returnObject = null;

        if (value != null) {
            ValueObject valueObject = null;
            ValueMap map = null;
            ValueMap subsetMap = ValueFactory.createMap();

            try {
                map = value.asMap();
            } catch (ValueTranslationException vte) {
                log.warn("Error extracting map from value: " + value);
            }

            if (map != null) {
                if (whitelist != null && whitelist.length > 0) {
                    for (String key : whitelist) {
                        if ((valueObject = map.get(key)) != null) {
                            subsetMap.put(key, valueObject);
                        }
                    }
                } else {
                    subsetMap = map;
                }

                if (blacklist != null && blacklist.length > 0) {
                    for (String key : blacklist) {
                        subsetMap.remove(key);
                    }
                }

                if (toString) {
                    returnObject = toString(subsetMap);
                } else {
                    returnObject = subsetMap;
                }
            }
        }

        return returnObject;
    }

    private ValueString toString(ValueMap subsetMap) {
        StringBuffer sb = new StringBuffer();

        for (ValueMapEntry valueMapEntry : subsetMap) {
            if (sb.length() == 0) {
                sb.append(valueMapEntry.getKey() + keySep + valueMapEntry.getValue().toString());
            } else {
                sb.append(valueSep + valueMapEntry.getKey() + keySep + valueMapEntry.getValue().toString());
            }
        }

        return ValueFactory.create(sb.toString());
    }
}

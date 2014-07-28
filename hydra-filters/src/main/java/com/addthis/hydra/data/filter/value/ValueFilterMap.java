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

import java.util.HashMap;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.util.JSONFetcher;


/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">uses the input as a key and returns the value
 * associated with the input in a specified map</span>.
 * <p/>
 * <p>The map is specified with the {@link #map map} field. Alternatively a JSON map
 * can be fetched from the internet using the {@link #mapURL mapURL} field. When no value is associated
 * with the input the default behavior is to return the input value. This behavior
 * can be changed to return null using the {@link #toNull toNull} field or to return
 * a default value using the {@link #defaultValue defaultValue} field.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   // english to french map
 *   {op:"map", map: {"hello" : "bonjour", "I" : "Je", "computer" : "ordinateur"}}
 * </pre>
 *
 * @user-reference
 * @hydra-name map
 */
public class ValueFilterMap extends StringFilter {

    /**
     * The map used to search for the input key.
     */
    @FieldConfig(codable = true)
    private HashMap<String, String> map;

    /**
     * If true, then the filter returns null when no value is associated with the input. Default
     * is false.
     */
    @FieldConfig(codable = true)
    private boolean toNull;

    /**
     * If non-null, then the filter returns defaultValue when no value is associated with the
     * input. Default is null.
     */
    @FieldConfig(codable = true)
    private String defaultValue;

    /**
     * Fetch the map from this URL.
     */
    @FieldConfig(codable = true)
    private String mapURL;

    /**
     * The timeout value when mapURL is used.
     */
    @FieldConfig(codable = true)
    private int httpTimeout = 60000;

    /**
     * If true, then print out a http trace when mapURL is used. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean httpTrace;

    public ValueFilterMap setMap(HashMap<String, String> map) {
        this.map = map;
        return this;
    }

    public ValueFilterMap setToNull(boolean toNull) {
        this.toNull = toNull;
        return this;
    }

    public ValueFilterMap setDefaultValue(String dv) {
        defaultValue = dv;
        return this;
    }

    public ValueFilterMap setMapURL(String url) {
        mapURL = url;
        return this;
    }

    public ValueFilterMap setHttpTimeout(int tmout) {
        httpTimeout = tmout;
        return this;
    }

    public ValueFilterMap setHttpTrace(boolean trace) {
        httpTrace = trace;
        return this;
    }

    @Override
    public void setup() {
        if (map == null && mapURL != null) {
            map = new JSONFetcher(httpTimeout, httpTrace).loadMap(mapURL);
        }
    }

    @Override
    public String filter(String value) {
        requireSetup();
        if (value != null) {
            if (map == null) {
                value = defaultValue != null ? defaultValue : toNull ? null : value;
            } else {
                String v = map.get(value);
                if (v != null) {
                    value = v;
                } else if (defaultValue != null) {
                    value = defaultValue;
                } else if (toNull) {
                    value = null;
                }
            }
            return value;
        } else {
            return value;
        }
    }
}

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

import java.util.HashMap;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.AutoParam;
import com.addthis.bundle.util.ConstantField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.data.util.JSONFetcher;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">uses the input as a key and returns the value
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
 *   {map: {"hello":"bonjour", "I":"Je", "computer" "ordinateur"}}
 * </pre>
 *
 * @user-reference
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ValueFilterMap extends AbstractValueFilterContextual implements SuperCodable {

    /** The map used to search for the input key. */
    @AutoParam private AutoField map;

    /** If true, then the filter returns null when no value is associated with the input. Default is false. */
    @JsonProperty private boolean toNull;

    /** If nonnull, then the filter returns defaultValue when no value is associated with the input. Default is null. */
    @JsonProperty private ValueObject defaultValue;

    /** Fetch the map from this URL. */
    @JsonProperty private String mapURL;

    /** The timeout value when mapURL is used. */
    @JsonProperty private int httpTimeout = 60000;

    /** If true, then print out a http trace when mapURL is used. Default is false. */
    @JsonProperty private boolean httpTrace;

    @VisibleForTesting
    ValueFilterMap() {}

    public ValueFilterMap setMap(HashMap<String, String> map) {
        ValueMap newMap = ValueFactory.createMap();
        map.forEach((k, v) -> {
            newMap.put(k, ValueFactory.create(v));
        });
        this.map = new ConstantField(newMap);
        return this;
    }

    public ValueFilterMap setToNull(boolean toNull) {
        this.toNull = toNull;
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
    public void postDecode() {
        if (map == null && mapURL != null) {
            setMap(new JSONFetcher(httpTimeout, httpTrace).loadMap(mapURL));
        }
    }

    @Override public void preEncode() {}

    private static final class ValidationOnly extends ValueFilterMap {
        @Override public void postDecode() {
            // intentionally do nothing
        }

        @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
            throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
        }
    }

    @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value, @Nullable Bundle context) {
        if (value != null) {
            if (map == null) {
                value = defaultValue != null ? defaultValue : toNull ? null : value;
            } else {
                ValueObject v = map.getValue(context).asMap().get(value.toString());
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

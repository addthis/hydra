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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.filter.value.ValueFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">extracts fields from a {@link ValueMap ValueMap}</span>.
 * <p/>
 * <p>This filter will extract values from a ValueMap and store them in the current bundle.
 * The mapping from the ValueMap to the bundle fields are specified using an
 * array of {@link XMap XMap} objects. Inside an XMap object, there is the name of a key
 * in the ValueMap, optionally a {@link ValueFilter ValueFilter} to perform on the value
 * associated with the key, and optionally a bundle field name that is assigned the value.
 * If the bundle field name is not specified, then the name of the ValueMap key is used
 * as the field of the bundle.  Extracted "from" fields that do not exist in the map are
 * ignored.  This filter returns true unless the "field" argument doesn't reference a
 * valid ValueMap.
 *
 * <p/>
 * <p>See {@link XMap} for an explanation on using indirection to retrieve keys.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   // extract params
 *   {op:"map-extract", field:"QUERY_PARAMS", map:[
 *        {from:"key1"},
 *        {from:"key2", to:"KEY2"},
 *        {from:"key3", to:"KEY3"},
 *   ]}
 * </pre>
 *
 * @user-reference
 * @hydra-name map-extract
 */
public class BundleFilterMapExtract extends BundleFilter {

    private static final Logger log = LoggerFactory.getLogger(BundleFilterMapExtract.class);

    /**
     * The name of the field that contains the ValueMap object. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String field;

    /**
     * The mapping from the ValueMap to the bundle format. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private XMap[] map;

    private String[] fields;

    @Override
    public void initialize() {
        fields = new String[]{field};
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField[] bound = getBindings(bundle, fields);
        ValueObject value = bundle.getValue(bound[0]);
        if (value == null) {
            return true;
        }
        ValueMap<?> mapValue;
        try {
            mapValue = value.asMap();
        } catch (ValueTranslationException vte) {
            log.warn("Error extracting map from value: " + value + " bundle: " + bundle);
            return false;
        }
        if (mapValue == null) {
            return true;
        }
        BundleFormat format = bundle.getFormat();
        for (XMap me : map) {
            String key = me.from;
            for (int i = 0; i < me.indirection && key != null; i++) {
                if (format.hasField(key)) {
                    key = bundle.getValue(format.getField(key)).asString().asNative();
                } else {
                    key = null;
                }
            }
            ValueObject val = mapValue.get(key);
            if (me.filter != null) {
                val = me.filter.filter(val);
            }
            if (val != null) {
                String toField = me.to != null ? me.to : me.from;
                bundle.setValue(format.getField(toField), val);
            }
        }
        return true;
    }

    /**
     * This class maps keys in the ValueMap to fields in the bundle format.
     * <p/>
     * <p>The {@link #indirection indirection} parameter can be used to interpret the
     * {@link #from} parameter using levels of indirection. For example if the
     * indirection parameter is set to '1', the 'from' parameter is set to 'abc',
     * and the bundle has field 'abc' with the value 'def' then the key 'def'
     * is used to perform the lookup into the map. When the "to" parameter is not
     * specified then the resulting value will always be stored in the
     * field specified by the "from" parameter, ie. storing the value ignores
     * the levels of indirection.</p>
     * <p/>
     * <p>Examples:</p>
     * <pre>
     *     {from:"uid"}, // copy value of "uid" key from map into "uid" field in the bundle
     *     {from:"uid", indirection:1}, // copy value of key specified in "uid" bundle into "uid"
     *     field in the bundle
     *     {from:"ln", to:"LANGUAGE"}, // copy value of "ln" key from map into "LANGUAGE" field in the bundle
     *     {from:"uf", to:"UID_FLAGS", filter:{op:"default",value:"notset"}},
     * </pre>
     *
     * @user-reference
     */
    public static final class XMap implements Codable {

        /**
         * The name of the key in the {@link ValueMap ValueMap}.
         */
        @FieldConfig(codable = true, required = true)
        private String from;

        /**
         * If non-null then assign the value to this field of the bundle.
         */
        @FieldConfig(codable = true)
        private String to;

        /**
         * If non-null then apply this filter on the value retrieved from the ValueMap. Default
         * is null.
         */
        @FieldConfig(codable = true)
        private ValueFilter filter;

        /**
         * A non-negative integer specifying the level of indirection.
         * Default is zero.
         */
        @FieldConfig(codable = true)
        private int indirection = 0;

        XMap setIndirection(int indirection) {
            this.indirection = indirection;
            return this;
        }


        XMap setFrom(String from) {
            this.from = from;
            return this;
        }
    }

    BundleFilterMapExtract setField(String field) {
        this.field = field;
        return this;
    }

    BundleFilterMapExtract setMap(XMap[] map) {
        this.map = map;
        return this;
    }
}

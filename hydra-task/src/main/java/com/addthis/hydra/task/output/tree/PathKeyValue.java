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
package com.addthis.hydra.task.output.tree;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.value.ValueFilter;

/**
 * This {@link PathElement PathElement} <span class="hydra-summary">creates one or more nodes using a specified key</span>.
 * <p/>
 * <p>This path element creates one or more nodes that are populated with values from the processed data.
 * The 'key' parameter specifies the name of a bundle field. A node is created for each unique value of the bundle field.
 * <p/>
 * <p>Compare this path element to the "{@link PathValue const}" path element. "const" creates exactly one
 * node with a specified value.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>paths : {
 *   "ROOT" : [
 *     {type:"const", value:"date"},
 *     {type:"value", key:"DATE_YMD"},
 *     {type:"value", key:"DATE_HH"},
 *   ],
 * },</pre>
 *
 * @user-reference
 * @hydra-name value
 */
public class PathKeyValue extends PathValue {

    public PathKeyValue() {
    }

    public PathKeyValue(String key) {
        this.key = key;
    }

    /**
     * Name of the bundle field that is used
     * to populate the constructed nodes.
     */
    @FieldConfig(codable = true)
    protected String key;

    /**
     * If non-null then apply this filter
     * onto the values before construction
     * of the nodes. Default is null.
     */
    @FieldConfig(codable = true)
    protected ValueFilter prefilter;

    private BundleField keyAccess;

    public String toString() {
        return this.getClass().getSimpleName() + "[" + key + "]";
    }

    @Override
    public void resolve(TreeMapper mapper) {
        super.resolve(mapper);
        keyAccess = mapper.bindField(key);
    }

    @Override
    public ValueObject getPathValue(TreeMapState state) {
        ValueObject v = getKeyValue(state.getBundle());
        if (prefilter != null) {
            v = prefilter.filter(v, state.getBundle());
        }
        return v;
    }

    /** */
    public final ValueObject getKeyValue(Bundle p) {
        try {
            ValueObject pv = null;
            if (keyAccess != null) {
                pv = p.getValue(keyAccess);
            }
            if (pv == null) {
                pv = value();
            }
            return pv;
        } catch (NullPointerException ex) {
            try {
                log.warn("NPE: keyAccess=" + keyAccess + " p=" + p + " in " + CodecJSON.encodeString(
                        this));
            } catch (Exception e) {
                e.printStackTrace();
            }
            throw ex;
        }
    }

    public final void setKeyValue(Bundle p, ValueObject value) {
        p.setValue(keyAccess, value);
    }

    public final void setKeyAccessor(BundleField nka) {
        keyAccess = nka;
    }

}

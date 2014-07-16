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

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.store.util.Raw;
import com.addthis.hydra.store.util.SeenFilterBasic;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">filters to elements seen or not seen by a basic bloom filter</span>.
 * <p/>
 * <p>{@link ValueFilterBloom ValueFilterBloom} is similar and uses a better bloom implementation,
 * but requires a lot work to make jobs 360 wrt gen/use of blooms.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name seen
 */
public class ValueFilterSeen extends ValueFilter {

    /**
     * If true, then return elements detected in the Bloom filter. Otherwise return elements
     * not detected in the Bloom filter. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean seen = true;

    /**
     * Retrieve the Bloom filter from a URL.
     */
    @FieldConfig(codable = true)
    private String url;

    /**
     * Apply this filter on the Bloom filter retrieved from a URL.
     */
    @FieldConfig(codable = true)
    private ValueFilter filter;

    /**
     * The bloom filter.
     */
    @FieldConfig(codable = true)
    protected SeenFilterBasic<Raw> bloom;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return value;
        }
        if (!initialize()) {
            return value;
        }
        boolean match = bloom.getSeen(Raw.get(ValueUtil.asNativeString(value)));
        if (seen) {
            return match ? value : null;
        }
        {
            return match ? null : value;
        }
    }

    private boolean initialize() {
        if (bloom == null && url != null) {
            SeenFilterBasic<Raw> newbloom = new SeenFilterBasic<>();
            String raw;
            try {
                byte[] bytes = ValueFilterHttpGet.httpGet(url, null, null, 30000, false);
                if (bytes == null) {
                    System.err.println("url " + url + " empty.  killing bloom filter");
                    url = null;
                    return false;
                }
                raw = Bytes.toString(bytes);
                if (filter != null) {
                    raw = ValueUtil.asNativeString(filter.filter(ValueFactory.create(raw)));
                }
                CodecJSON.decodeString(newbloom, raw);
                bloom = newbloom;
                return true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            return bloom != null;
        }
    }
}

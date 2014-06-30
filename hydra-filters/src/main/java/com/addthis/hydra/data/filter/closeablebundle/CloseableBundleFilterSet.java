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
package com.addthis.hydra.data.filter.closeablebundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;


public class CloseableBundleFilterSet extends CloseableBundleFilter {

    @FieldConfig(codable = true, required = true)
    private String value;
    @FieldConfig(codable = true, required = true)
    private String to;
    @FieldConfig(codable = true)
    private CloseableBundleFilter filter;
    @FieldConfig(codable = true)
    private boolean not;

    private String fields[];

    @Override
    public void initialize() {
        fields = new String[]{to};
        if (filter != null) {
            filter.initOnceOnly();
        }
    }

    @Override
    public void close() {
        if (filter != null) {
            filter.close();
        }
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        boolean success = true;
        BundleField bound[] = getBindings(bundle, fields);

        if (filter != null) {
            success = filter.filter(bundle);
        }

        if (success) {
            bundle.setValue(bound[0], ValueFactory.create(value));
            return !not;
        } else {
            return not;
        }
    }
}

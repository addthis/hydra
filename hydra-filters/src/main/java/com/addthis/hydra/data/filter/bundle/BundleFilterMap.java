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
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">executes a sequence of {@link BundleFilterField BundleFilterField} operations</span>.
 * <p/>
 * <p>A BundleFilterField runs a value filter on a specific field.
 * The value filter is run on the value stored at the location of the {@link BundleFilterField#from from}
 * field. If the {@link BundleFilterField#to to} field is specified then the output
 * of the filter operation is stored in that field. Otherwise the
 * from field is updated with the output of the filter operation.
 * If the value filter is not specified then the value is copied
 * with no filtering.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"map", fields:[
 *        {from: "DATE", to: "DATE_YMD", filter: {op: "slice", to: 6}},
 *        {from: "DATE", to: "DATE_HOUR", filter: {op: "slice", from: 7, to: 9}},
 *   ]},
 * </pre>
 *
 * @user-reference
 * @hydra-name map
 */
public class BundleFilterMap extends BundleFilter {

    /**
     * The sequence of field bundle filters to execute.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilterField[] fields;

    /**
     * If true then exit on first operation that fails and return false. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean exitFail;

    /**
     * Specifies the {@link BundleFilterField#nullFail nullFail} field for all the component
     * filters. Default is false.
     */
    @FieldConfig(codable = true)
    private Boolean nullFail;

    @Override
    public void open() {
        for (BundleFilterField f : fields) {
            f.open();
            if (nullFail != null) {
                f.setNullFail(nullFail);
            }
        }
    }

    @Override
    public boolean filter(Bundle bundle) {
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].filter(bundle) && exitFail) {
                return false;
            }
        }
        return true;
    }
}

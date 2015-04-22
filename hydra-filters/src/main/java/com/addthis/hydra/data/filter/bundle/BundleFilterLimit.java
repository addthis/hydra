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
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">returns true for at most N iterations</span>.
 * <p/>
 * <p>This filter will return true for the first {@link #limit limit} rows of data.
 * Subsequent rows will yield false.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *    {limit.limit:10},
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterLimit implements BundleFilter {

    /**
     * The number of rows to permit. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int limit;

    @Override
    public boolean filter(Bundle row) {
        return limit-- > 0;
    }

}

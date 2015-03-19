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
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">provides if/then/else branching logic with optional looping</span>.
 * <p/>
 * <p>The test bundle filter is executed and if it returns true then the onTrue bundle filter
 * is executed. If the test bundle filter returns false then the onFalse bundle filter
 * is executed. This process is repeated at most {@link #loop loop} iterations.
 * If the result of onTrue or onFalse is false then the computation is terminated and
 * the filter returns false. Otherwise the filter returns true. </p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterTest implements BundleFilter {

    /**
     * The bundle filter that is queried for a true or false return value. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter test;

    /**
     * The bundle filter that is executed when test returns true. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter onTrue;

    /**
     * The bundle filter that is executed when test returns false. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter onFalse;

    /**
     * The maximum number of iterations to perform. Use -1 to iterate up to {@link
     * Integer#MAX_VALUE} times. Default is 1.
     */
    @FieldConfig(codable = true)
    private int loop = 1;

    @Override
    public boolean filter(Bundle row) {
        boolean ret = false;
        do {
            if (test.filter(row)) {
                ret = onTrue.filter(row);
            } else {
                ret = onFalse.filter(row);
            }
        }
        while (ret && --loop != 0);
        return ret;
    }

}

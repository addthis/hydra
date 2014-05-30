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
import com.addthis.codec.Codec;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">conditionally executes a bundle filter</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"condition",
 *      ifCondition: {op:"contains", field:"SRC", value:["foo", "bar"]},
 *      ifDo: {op:"clear", field:"SRC"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name condition
 */
public class BundleFilterCondition extends BundleFilter {

    /**
     * The conditional bundle filter. This field is required.
     */
    @Codec.Set(codable = true, required = true)
    BundleFilter ifCondition;

    /**
     * The bundle filter to execute when {@link #ifCondition ifCondition} returns true. This field is required.
     */
    @Codec.Set(codable = true)
    BundleFilter ifDo;

    /**
     * The bundle filter to execute when {@link #ifCondition ifCondition} returns false. This field is optional.
     */
    @Codec.Set(codable = true)
    BundleFilter elseDo;

    @Override
    public void initialize() {
        ifCondition.initOnceOnly();
        if (ifDo != null) {
            ifDo.initOnceOnly();
        }
        if (elseDo != null) {
            elseDo.initOnceOnly();
        }
    }

    @Override
    public boolean filterExec(Bundle row) {
        if (row != null) {
            if (ifCondition != null && ifCondition.filterExec(row)) {
                if (ifDo != null) {
                    return ifDo.filterExec(row);
                }
            } else {
                if (elseDo != null) {
                    return elseDo.filterExec(row);
                }
            }
        }
        return true;
    }
}

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
package com.addthis.hydra.data.filter.bundle.unary;

import javax.annotation.Nullable;

import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.filter.util.UnaryOperation;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * <p>This {@link BundleFilter BundleFilter} <span class="hydra-summary">wraps another bundle filter and always returns
 * true</span>.</p>
 * <p>Example: wrap a chain so it never fails</p>
 * <pre>
 *     {safely:[
 *         {has:"SOME_FIELD}
 *         {from:"SOME_FIELD", to:"SOME_FLAG"}
 *     ]}
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterSafely extends BundleFilterUnary {

    @JsonCreator public BundleFilterSafely(@Nullable BundleFilter filter) {
        super(UnaryOperation.TRUE, filter);
    }
}

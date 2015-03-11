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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.filter.util.UnaryOperation;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p></p>
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">runs a unary operation on the specified filter result</span>.
 * <p/>
 * <p>The valid operations are <code>true</code>, <code>false</code>, <code>identity</code>, and <code>negation</code>.
 * <code>true</code> will always return true, with the opposite for <code>false</code>.
 * <code>negation</code> will return the opposite of the filter return value.
 * </p>
 * <p>If there is no filter specified, the operation will be performed on the value <code>true</code>.
 * </p>
 * <p>You should probably use the aliases that call this plugin:
 * <ul>
 *     <li>Return true:
 *     <ul>
 *         <li>true</li>
 *         <li>safely</li>
 *         <li>ignore</li>
 *     </ul></li>
 *
 *     <li>Return false:
 *     <ul>
 *         <li>false</li>
 *         <li>fail</li>
 *         <li>then fail</li>
 *     </ul></li>
 *
 *     <li>Return opposite of filter:
 *     <ul>
 *         <li>invert</li>
 *         <li>is not</li>
 *         <li>isn't</li>
 *     </ul></li>
 * </ul>
 * </p>
 *
 * @user-reference
 */
public class BundleFilterUnary implements BundleFilter {

    @Nonnull  private final UnaryOperation operation;
    @Nullable private final BundleFilter   filter;

    protected BundleFilterUnary(@Nonnull @JsonProperty("operation") UnaryOperation operation,
                                @Nullable @JsonProperty("filter")   BundleFilter filter) {
        this.operation = checkNotNull(operation);
        this.filter = filter;
    }

    @Override public boolean filter(Bundle row) {
        boolean filterResult = (filter == null) || filter.filter(row);
        return operation.compute(filterResult);
    }
}

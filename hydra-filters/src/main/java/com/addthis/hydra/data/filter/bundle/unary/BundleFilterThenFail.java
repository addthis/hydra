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
 * <p>This {@link BundleFilter BundleFilter} <span class="hydra-summary">runs the specified filter and returns false</span>.</p>
 * <p>Example: Write a debug message when you find a bundle you don't like, but fail the bundle</p>
 * <pre>
 *     [
 *         {has:"BAD_BUNDLE_FLAG"}
 *         {then fail.debug {}}
 *     ]
 * </pre>
 * <p>One thing you can do with <code>then fail</code> that you can't with <code>fail</code> or <code>false</code>
 * is to use the <code>field</code> bundle filter (the default bundle filter if you don't specify a name) without
 * using its name, just like you can in a hydra job. For fail and false you must specify "field" as your filter.</p>
 * <p>Example:</p>
 * <pre>
 *     {then fail:{from:"FIELD", some-filter {}}}
 *     {fail.field:{from:"FIELD", some-filter {}}}
 *     {false.field:{from:"FIELD", some-filter {}}}
 * </pre>
 * <p>However, one thing <code>fail</code> and <code>false</code> can do that you can't do with <code>then fail</code>
 * is call them with empty braces.</p>
 * <p>Example:</p>
 * <pre>
 *    {fail {}}
 *    {false {}}
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterThenFail extends BundleFilterUnary {

    @JsonCreator public BundleFilterThenFail(@Nullable BundleFilter filter) {
        super(UnaryOperation.FALSE, filter);
    }
}

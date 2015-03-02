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

import java.util.function.Predicate;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.codec.annotations.Pluggable;

/**
 * A bundle filter applies a transformation on a bundle and returns
 * true or false to indicate whether the transformation was successful.
 *
 * @user-reference
 * @hydra-category Bundle Filters
 * @hydra-doc-position 3
 */
@FunctionalInterface
@Pluggable("bundle-filter")
public interface BundleFilter extends Predicate<Bundle> {

    /**
     * @param bundle      row/line/packet bundle
     * @param bindTargets use the same object or re-binding will occur
     * @return bound field wrappers
     */
    static BundleField[] getBindings(final Bundle bundle, final String[] bindTargets) {
        BundleField[] boundFields = null;
        if (bindTargets != null) {
            BundleFormat format = bundle.getFormat();
            BundleField[] bindings = new BundleField[bindTargets.length];
            for (int i = 0; i < bindTargets.length; i++) {
                if (bindTargets[i] != null) {
                    bindings[i] = format.getField(bindTargets[i]);
                }
            }
            boundFields = bindings;
        }
        return boundFields;
    }

    @Override default boolean test(Bundle row) {
        return filter(row);
    }

    /* returns true if chain should continue, false to break */
    abstract boolean filter(Bundle row);
}

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
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.codables.SuperCodable;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * A bundle filter applies a transformation on a bundle and returns
 * true or false to indicate whether the transformation was successful.
 *
 * @user-reference
 * @hydra-category
 */
@Pluggable("bundle-filter")
public abstract class BundleFilter implements Codable {

    /**
     * @param bundle      row/line/packet bundle
     * @param bindTargets use the same object or re-binding will occur
     * @return bound field wrappers
     */
    protected final BundleField[] getBindings(final Bundle bundle, final String[] bindTargets) {
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

    /**
     * The recommended pattern for initialization of BundleFilters is to
     * use {@link JsonCreator} style constructors. See {@link BundleFilterNum}
     * for an example of this pattern. This construction allows one to mark any
     * appropriate fields as final fields which is a recommended best practice.
     *
     * BundleFilters may be instantiated and then never used again. For example
     * this happens when we validate a job configuration in the user interface
     * when a job is saved. Any stateful or expensive initialization, such as
     * contacting a resource on the network or writing to a disk, should happen
     * in this open method. Do not use {@link SuperCodable} for initialization
     * of BundleFilters that is deprecated in favor of the approach described here.
     *
     * Any application using a BundleFilters must explicitly invoke the open
     * method exactly once after the object has been initialized and before
     * it is used. The best practice is to call the open method even if you
     * know it performs no operation, as a future-proof for any changes to
     * your application.
     */
    public abstract void open();

    /* returns true if chain should continue, false to break */
    public abstract boolean filter(Bundle row);

}

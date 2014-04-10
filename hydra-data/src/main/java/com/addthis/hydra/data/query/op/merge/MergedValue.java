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

package com.addthis.hydra.data.query.op.merge;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.hydra.data.query.op.MergedRow;

/**
 * Both a Merge Operation (basically a function with a small amount of state),
 * and a bundle format field bridge between two formats.
 */
public interface MergedValue {

    /**
     * Access a value from nextBundle and merges it with a value in mergedRow.
     * Currently it is at most one field from each bundle, but more could be
     * interacted with if the implementation is careful.
     *
     * Note that this does _not_ merge the entire bundle like a similar method
     * on MergedRow does. The method uses this signature in order to encapsulate
     * bundle field lookup logic and provide a little more flexibility to the
     * internals if needed (the latter being the reason MergedRow is used explicitly
     * in place of Bundle).
     */
    public void merge(Bundle nextBundle, MergedRow mergedRow);

    /**
     * Signals the end of additional bundles/values to merge. May set a value
     * in mergedRow based on accumulated data.
     */
    public void emit(MergedRow mergedRow);

    /**
     * This field/value should be used when controlling which rows to merge. Currently
     * only used by KeyValue, which makes it basically 'instanceof KeyValue', but putting
     * this here is a little more readable, provides some future options, and better
     * protects against changing api / structure in the future (where eg. KeyValue may
     * no longer exist as a class).
     */
    public boolean isKey();

    /**
     * Getters and setters for the object fields that are used by merge/emit to access
     * MergedRows and Bundles. They provide the bridging logic, and could be spun into
     * their own interface, but MergedValue would basically have to extend it because
     * it is a little disingenuous to suggest it should can or should easily be used
     * without it.
     */

    public BundleField getFrom();

    public BundleField getTo();

    public void setFrom(BundleField from);

    public void setTo(BundleField to);

}
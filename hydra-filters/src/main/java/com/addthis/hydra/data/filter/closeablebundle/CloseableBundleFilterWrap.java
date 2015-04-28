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
package com.addthis.hydra.data.filter.closeablebundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CloseableBundleFilterWrap implements CloseableBundleFilter {

    /**
     * The regular filter to use in the context of a closeable filter.
     */
    private final BundleFilter filter;

    @JsonCreator
    public CloseableBundleFilterWrap(@JsonProperty(value = "filter", required = true) BundleFilter filter) {
        this.filter = filter;
        if (filter instanceof CloseableBundleFilter) {
            throw new IllegalStateException("'filter' argument cannot be a CloseableBundleFilter");
        }
    }

    public void close() {}

    @Override public boolean filter(Bundle row) {
        return filter.filter(row);
    }
}

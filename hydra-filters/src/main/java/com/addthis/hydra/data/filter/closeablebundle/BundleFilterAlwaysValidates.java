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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BundleFilterAlwaysValidates implements CloseableBundleFilter {
    private static final Logger log = LoggerFactory.getLogger(BundleFilterAlwaysValidates.class);

    @Override public boolean filter(Bundle row) {
        throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
    }

    @Override public void close() {
        throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
    }
}

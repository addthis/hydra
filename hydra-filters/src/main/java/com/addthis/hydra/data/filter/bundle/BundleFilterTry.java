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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Performs the required filter try (or sometimes 'tryDo'). If and only if it fails,
 * then the catch filter is performed and its result is returned instead. The default
 * is usually to set catchDo to a no-op filter that always returns true. Reasonable
 * alternatives include logging (via filter debug or setting a specific field),
 * conditionally rethrowing with a filter that may fail, and imagination.
 *
 * @user-reference
 * @hydra-name try
 */
public class BundleFilterTry extends BundleFilter {

    @JsonProperty("try")
    @FieldConfig(required = true)
    BundleFilter tryDo;

    @JsonProperty("catch")
    @FieldConfig(required = true)
    BundleFilter catchDo;

    @Override
    public void open() {
        tryDo.open();
        if (catchDo != null) {
            catchDo.open();
        }
    }

    @Override
    public boolean filter(Bundle row) {
        boolean tryResult = tryDo.filter(row);
        return tryResult || (catchDo == null) || catchDo.filter(row);
    }
}

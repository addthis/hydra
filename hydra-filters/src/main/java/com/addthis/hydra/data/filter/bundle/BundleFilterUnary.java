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

import javax.annotation.Nullable;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.util.UnaryOperation;

public class BundleFilterUnary extends BundleFilter {

    @FieldConfig(required = true) private UnaryOperation operation;
    @FieldConfig @Nullable        private BundleFilter   filter;

    @Override public void initialize() {
        if (filter != null) {
            filter.initOnceOnly();
        }
    }

    @Override public boolean filterExec(Bundle row) {
        boolean filterResult = (filter == null) || filter.filterExec(row);
        return operation.compute(filterResult);
    }
}

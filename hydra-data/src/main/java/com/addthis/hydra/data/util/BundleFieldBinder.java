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
package com.addthis.hydra.data.util;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueObject;

/**
 * manages binding and rebinding bundle fields
 */
public class BundleFieldBinder {

    private BundleFormat format;
    private BundleField field;

    public ValueObject getValue(String fieldName, Bundle bundle) {
        BundleFormat currentFormat = bundle.getFormat();
        if (format != currentFormat) {
            field = currentFormat.getField(fieldName);
        }
        return bundle.getValue(field);
    }
}

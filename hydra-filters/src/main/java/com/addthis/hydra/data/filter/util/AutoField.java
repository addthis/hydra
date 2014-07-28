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
package com.addthis.hydra.data.filter.util;

import javax.annotation.concurrent.ThreadSafe;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.config.ValueCodable;

import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

/**
 * A convenience class for codable objects that might otherwise have to implement
 * their own bundle field binding and caching. It might be nice to expand to options
 * for different native type conversions and inter-value object type conversions.
 *
 * When using CodecConfig, you would simply replace the String fields with AutoField
 * fields of the same name, and then use the AutoField object to get, set, remove values
 * and so on.
 */
@ThreadSafe
public class AutoField implements ValueCodable {

    @FieldConfig private String name;

    /* always check before using and always copy to a local variable */
    private transient BundleField cachedField;

    private BundleField checkAndGet(Bundle bundle) {
        BundleField currentField = cachedField;
        if ((currentField != null)
            && (currentField.getIndex() != null) /* kv bundles ruin everything */
            && (bundle.getFormat().getField(currentField.getIndex()) == currentField)) {
            return currentField;
        } else {
            BundleField newField = bundle.getFormat().getField(name);
            cachedField = newField;
            return newField;
        }
    }

    public ValueObject getValue(Bundle bundle) {
        BundleField field = checkAndGet(bundle);
        return bundle.getValue(field);
    }

    public void setValue(Bundle bundle, ValueObject value) {
        BundleField field = checkAndGet(bundle);
        bundle.setValue(field, value);
    }

    public void removeValue(Bundle bundle) {
        BundleField field = checkAndGet(bundle);
        bundle.removeValue(field);
    }

    /** should be called at most once during construction - thread safety is void otherwise */
    @Override public void fromConfigValue(ConfigValue configValue, ConfigObject defaults) {
        name = (String) configValue.unwrapped();
    }
}

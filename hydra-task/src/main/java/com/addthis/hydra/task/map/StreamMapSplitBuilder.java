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
package com.addthis.hydra.task.map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This StreamBuilder splits a ValueMap amongst multiple bundles.
 * Each bundle will have the same fields as the original with the exception
 * of the field specified to use as the map, which won't be duplicated
 * and will have one entry in each bundle with names as specified by keyField and valueField.
 */
public class StreamMapSplitBuilder extends StreamBuilder {
    /** The map field to split into multiple bundles */
    @FieldConfig(required = true) private String field;
    /** The field name in which to place each key in the new bundle */
    @FieldConfig(required = true) private String keyField;
    /** The field name in which to place each value in the new bundle */
    @FieldConfig(required = true) private String valueField;

    @Override
    public void init() {}

    @Override
    public void process(Bundle bundle, StreamEmitter emitter) {
        ValueObject obj = bundle.getValue(bundle.getFormat().getField(field));
        if (obj != null && obj.getObjectType() == ValueObject.TYPE.MAP) {
            ValueMap map = obj.asMap();
            for (ValueMapEntry entry : map) {
                Bundle newBundle = bundle.createBundle();
                BundleFormat format = newBundle.getFormat();
                for (BundleField bundleField : format) {
                    // do not add map field to the new bundle, but do add all others
                    if (!bundleField.getName().equals(obj)) {
                        newBundle.setValue(bundleField, bundle.getValue(bundleField));
                    }
                }
                newBundle.setValue(format.getField(keyField), ValueFactory.create(entry.getKey()));
                newBundle.setValue(format.getField(valueField), entry.getValue());
                emitter.emit(newBundle);
            }
        }
    }
}
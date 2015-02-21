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
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;


public class StreamRowSplitBuilder extends StreamBuilder {

    @FieldConfig(codable = true)
    private String field;
    @FieldConfig(codable = true)
    private ValueFilter filter;
    @FieldConfig(codable = true)
    private String[] splitOutputFields;
    @FieldConfig(codable = true)
    private boolean dropFromOriginal;

    @Override
    public void init() {
    }

    @Override
    public void process(Bundle bundle, StreamEmitter emitter) {
        ValueObject valueObject = bundle.getValue(bundle.getFormat().getField(field));
        if (filter != null) {
            valueObject = filter.filter(valueObject, bundle);
        }
        if (valueObject != null && valueObject.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray array = valueObject.asArray();
            for (ValueObject v : array) {
                Bundle newBundle = bundle.createBundle();
                for (String outputFieldString : splitOutputFields) {
                    BundleField outputField = newBundle.getFormat().getField(outputFieldString);
                    if (outputFieldString.equals(field)) {
                        newBundle.setValue(outputField, v);
                    } else {
                        newBundle.setValue(outputField, bundle.getValue(outputField));
                    }
                }
                emitter.emit(newBundle);
            }
        }
        if (dropFromOriginal) {
            bundle.removeValue(bundle.getFormat().getField(field));
        }
        emitter.emit(bundle);
    }
}

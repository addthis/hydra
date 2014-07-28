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
import com.addthis.hydra.data.filter.bundle.BundleFilter;

/**
 * emits a bundle for "each" value of an array element
 */
public class EachStreamBuilder extends StreamBuilder {

    @FieldConfig(codable = true)
    private String fromArrayField;
    @FieldConfig(codable = true)
    private String toField;
    @FieldConfig(codable = true)
    private BundleFilter emitFilter;

    @Override
    public void init() {
        // no init for this builder
    }

    @Override
    public void process(Bundle row, StreamEmitter emitter) {
        BundleField fieldFrom = row.getFormat().getField(fromArrayField);
        BundleField fieldTo = row.getFormat().getField(toField);
        ValueObject fromObject = row.getValue(fieldFrom);
        if (fromObject == null || fromObject.getObjectType() != ValueObject.TYPE.ARRAY) {
            if (emitFilter == null || emitFilter.filter(row)) {
                emitter.emit(row);
            }
        } else {
            ValueArray fromArray = fromObject.asArray();
            for (ValueObject element : fromArray) {
                Bundle newBundle = row.createBundle();
                for (BundleField field : row.getFormat()) {
                    newBundle.setValue(field, row.getValue(field));
                }
                newBundle.setValue(fieldTo, element);
                if (emitFilter == null || emitFilter.filter(newBundle)) {
                    emitter.emit(newBundle);
                }
            }
        }
    }
}

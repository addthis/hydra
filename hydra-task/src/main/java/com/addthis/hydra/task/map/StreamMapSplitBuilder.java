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
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This StreamBuilder splits a ValueMap amongst multiple bundles.
 * Each bundle will have the same fields as the original with the exception
 * of the field specified to use as the map, which won't be duplicated
 * and will have one entry in each bundle with names as specified by keyField and valueField.
 */
public class StreamMapSplitBuilder extends StreamBuilder {
    /** The map field to split into multiple bundles */
    @JsonProperty(required = true) private AutoField field;
    /** The field name in which to place each key in the new bundle */
    @JsonProperty(required = true) private AutoField keyField;
    /** The field name in which to place each value in the new bundle */
    @JsonProperty(required = true) private AutoField valueField;
    /** If true, a copy of the bundle without any key and value set will also be emitted
     * (map field will also be excluded)
     * The bundle will be emitted even if there is no map field in the bundle. */
    @JsonProperty private boolean emitOriginal = false;
    
    @Override
    public void init() {}

    @Override
    public void process(Bundle bundle, StreamEmitter emitter) {
        ValueObject obj = field.getValue(bundle);
        field.removeValue(bundle);
        if ((obj != null) && (obj.getObjectType() == ValueObject.TYPE.MAP)) {
            ValueMap map = obj.asMap();
            for (ValueMapEntry entry : map) {
                Bundle newBundle = Bundles.deepCopyBundle(bundle);
                keyField.setValue(newBundle, ValueFactory.create(entry.getKey()));
                valueField.setValue(newBundle, entry.getValue());
                emitter.emit(newBundle);
            }
        }
        if (emitOriginal) {
            emitter.emit(bundle);
        }
    }
}
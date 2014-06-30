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

import java.util.HashMap;
import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * This is a util to join 2 datasets with similar keys
 * This is intended to run on a single thread and hence is not thread safe
 */
public class StreamJoin extends StreamBuilder {

    @FieldConfig(codable = true, required = true)
    private String key;
    @FieldConfig(codable = true, required = true)
    private String[] fields;
    @FieldConfig(codable = true, required = true)
    private String hash;

    private Map<String, ListBundle> keyMap;
    private String currentHash;
    private int numExpectedFields;

    private static final Logger logger = LoggerFactory.getLogger(StreamJoin.class);

    @Override
    public void init() {
        numExpectedFields = fields.length;
        keyMap = new HashMap<String, ListBundle>(10000, 0.75f);
    }

    @Override
    public void process(Bundle row, StreamEmitter emitter) {
        String keyValue = row.getValue(row.getFormat().getField(key)).asString().getString();
        String hashValue = row.getValue(row.getFormat().getField(hash)).asString().getString();

        if (keyValue != null) {
            if (currentHash != null && hashValue != null && !currentHash.equals(hashValue)) {
                releaseMap(emitter);
            }

            currentHash = hashValue;

            joinAndEmit(keyValue, row, emitter);
        }
    }

    private synchronized void releaseMap(StreamEmitter emitter) {
        for (ListBundle bundle : keyMap.values()) {
            if (bundle.size() == numExpectedFields) {
                emitter.emit(bundle);
            }
        }

        keyMap = new HashMap<String, ListBundle>(10000, 0.75f);
    }

    private void joinAndEmit(String keyValue, Bundle row, StreamEmitter emitter) {
        ListBundle bundle = null;
        ValueObject fieldValue = null;
        BundleField bundleField = null;
        ListBundleFormat form = new ListBundleFormat();
        boolean newBundle = false;

        if ((bundle = keyMap.get(keyValue)) == null) {
            bundle = new ListBundle();
            keyMapPut(keyValue, bundle);
            newBundle = true;
        }

        for (String field : fields) {
            bundleField = row.getFormat().getField(field);

            if (bundleField != null) {
                fieldValue = row.getValue(bundleField);
            }

            if (fieldValue != null) {
                bundleField = form.getField(field);
                bundle.setValue(bundleField, fieldValue);
            }
        }

        if (!newBundle && bundle.size() == numExpectedFields) {
            emitBundle(keyValue, bundle, emitter);
        }
    }

    private synchronized void keyMapPut(String keyValue, ListBundle bundle) {
        keyMap.put(keyValue, bundle);
    }

    private void emitBundle(String keyValue, ListBundle bundle, StreamEmitter emitter) {
        if (bundle != null) {
            emitter.emit(bundle);
            // remove from map
            keyMapRemove(keyValue);
        }
    }

    private synchronized void keyMapRemove(String keyValue) {
        keyMap.remove(keyValue);
    }

    @Override
    public void streamComplete(StreamEmitter streamEmitter) {
        if (keyMap.size() > 0) {
            for (String key : keyMap.keySet()) {
                ListBundle bundle = keyMap.get(key);

                if (bundle.size() == numExpectedFields) {
                    emitBundle(key, bundle, streamEmitter);
                }
            }
        }
    }
}

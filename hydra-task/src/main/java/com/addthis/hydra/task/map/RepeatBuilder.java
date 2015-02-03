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
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueTranslationException;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts a numeric field from a bundle, then emits a copy of the bundle the specified number of times.
 */
public class RepeatBuilder extends StreamBuilder {
    private static final Logger log = LoggerFactory.getLogger(RepeatBuilder.class);

    /** The field describing how many times to emit each bundle */
    @JsonProperty(required = true) private AutoField repeatField;

    /** A default number of times to emit if the specified field is missing or can't be parsed */
    @JsonProperty private long defaultRepeats = 0;

    /** If true, escalate parse failures rather than using the default repeat number */
    @JsonProperty private boolean failOnParseException = false;

    /** If true, perform a deep-copy on each input bundle prior to sending it downstream */
    @JsonProperty private boolean copyBundle = false;

    @Override
    public void init() {}

    @Override
    public void process(Bundle row, StreamEmitter emitter) {
        long emitTimes = defaultRepeats;
        ValueObject repeatCount = repeatField.getValue(row);
        if (repeatCount != null) {
            try {
                emitTimes = repeatCount.asLong().getLong();
            } catch (ValueTranslationException vte) {
                if (failOnParseException) {
                    throw vte;
                } else {
                    log.warn("Failed to parse repeats field value={}", repeatCount.asString().asNative());
                }
            }
        }
        for (int i = 0; i < emitTimes; i++) {
            Bundle toEmit = copyBundle ? Bundles.deepCopyBundle(row) : row;
            // Copy the bundle each time to ensure sanity downstream
            emitter.emit(toEmit);
        }
    }
}

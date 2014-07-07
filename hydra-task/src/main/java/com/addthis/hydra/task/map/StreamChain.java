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
import com.addthis.codec.annotations.FieldConfig;

public class StreamChain extends StreamBuilder {

    /**
     * The chain of stream builders to execute.
     */
    @FieldConfig(codable = true, required = true)
    private StreamBuilder builders[];

    @Override
    public void init() {
        for (StreamBuilder builder : builders) {
            builder.init();
        }
    }

    @Override
    public void process(Bundle row, StreamEmitter emitter) {
        for (StreamBuilder builder : builders) {
            builder.process(row, emitter);
        }
    }
}
